# Modules that will be used on the script
# Made sure that we use modules that are pre-installed on python 2.6 to avoid adding modules via pip
# as most customers don't have internet connection or is blocked on the main server.
import sys, os, getopt, logging, subprocess, csv, glob, re, json, time
from operator import itemgetter
from datetime import datetime
from time import gmtime, strftime


# Class used to store all the variables used throughout the script
# so that its one place stop if we need to change anything on the future.
class VariableClass():
    def __init__(self):
        # Version
        self.version = 1.0

        # Script logging process format and logger
        self.loggerformats = '%(asctime)s [%(levelname)s] %(message)s'
        self.logger = logging.getLogger(__file__)

        # The below parameters are the location where in the csv file for date, pid, duration, query, dumpinfo
        self.row_date = 0
        self.row_pid = 3
        self.row_duration = 18
        self.row_query = 24
        self.row_dumplocation = 18

        # Default parameters to be set for script arguments.
        self.StartTime = None
        self.EndTime = None
        self.filename = None
        self.contents = None
        self.hostmapdates = None
        self.debug = 0

        # Time conversion variables
        self.msTOsec = 0.001
        self.msTOmin = 0.00001667
        self.timeformatSec = 's'
        self.timeformatmin = 'm'

        # In case in future you want some other specific time format
        # just change the below variable with the variables mentioned above
        # Default is set to seconds
        self.timeConvertor = self.msTOsec
        self.timeFormat = self.timeformatSec

        # Size of the dump format
        self.bytestoMB = 1048576
        self.bytestoGB = 1073741824
        self.sizeformatMB = 'MB'
        self.sizeformatGB = 'GB'

        # In case in future you want some other specific size format
        # just change the below variable with the variables mentioned above
        # Default is set to MegaBytes
        self.sizeConvertor = self.bytestoMB
        self.sizeFormat = self.sizeformatMB

        # hostmap builder query
        self.HealthCheckquery = "psql -d template1 -Atc \" select 1 \" &>/dev/null"
        self.query = "SELECT hostname ||':' || f.fselocation ||'/pg_log:' || dbid ||':' || content" \
        " FROM pg_filespace_entry f , pg_tablespace t , gp_segment_configuration c" \
        " WHERE f.fsefsoid=t.spcfsoid" \
        " AND c.dbid=f.fsedbid" \
        " AND t.oid=1663 " \
        " AND role = 'p'"

        # Directory where the files will be temporary copied to
        self.tempdir = "/tmp/wrkdir_{0}".format(__file__)

        # Format used in the scripts to verify the date argument
        self.StartEnddate_format = '%Y-%m-%d  %H:%M:%S'
        self.hostmapdate_format = '%Y-%m-%d'

        # Output File name
        self.InputFile = "gpdb" + "_" + __file__ + "_" + strftime("%Y%m%d%H%M%S", gmtime()) + ".csv"
        self.OutputFile = __file__ + "_" + strftime("%Y%m%d%H%M%S", gmtime()) + ".out"
        self.hostmapfile = "hostmap"

        # Format used to format the output of the script
        self.SQLfmt1 = '|{0:-<60}|{0:->30}|{0:->30}|{0:->15}|{0:->20}|{0:->15}|{0:->15}|{0:->20}|'
        self.SQLfmt2 = '|{0:<60}|{1:>30}|{2:>30}|{3:>15}|{4:>20}|{5:>15}|{6:>15}|{7:>20}|'
        self.SQLfmt3 = '|{0:<60}|{1:>30}|{2:>30}|{3:>15}|{4:>20.2f}|{5:>15.2f}|{6:>15.2f}|{7:>20.2f}|'
        self.SQLfmt4 = '|{0:<60}|{1:>30}|{2:>30}|{3:>15}|{4:>20.2f}|{5:>15}|{6:>15}|{7:>20}|'
        self.Copyfmt1 = '|{0:<60}|{1:>15}|'
        self.Copyfmt2 = '|{0:->60}|{0:->15}|'
        self.Copyfmt3 = '|{0:<60}|{1:>15.2f}|'
        self.date_format = "%Y-%m-%d %H:%M:%S.%f"

        # Copy Output Formatter variables
        self.Stopper = 0
        self.TotalofTotal = 0

        # Output log Heading
        self.MasterLogHeading = "{0:>100}X-----BACKUP TIME SUMMARY FOR MASTER HOST----X"
        self.SegmentLogHeading = "{0:>100}X-----BACKUP TIME SUMMARY FOR SEGMENT HOST: {1}----X"

        # The description of the columns outputted by the script
        self.ColumnDescription = "The description of column in the output log are below \n\n" \
                                 "Statement          : The statement executed by the process." + "\n" + \
                    "                             - ShareLock PID select are trimmed to 28 characters" + "\n" + \
                    "                             - All LOCK queries by the process are clubbed to single statement, so X can be any table." + "\n" + \
                    "                             - All COPY command by the process are clubbed to single statement, so X can be any table." + "\n" + \
                    "                             - All SET SEARCH_PATH by the process are clubbed to single statement, so X can be any schema." + "\n" + \
                    "First Run          : The First time this query was executed." + "\n" + \
                    "Last Run           : The Last time this query was executed." + "\n" + \
                    "# of Execution     : The total number of times this query was executed." + "\n" + \
                    "Total Exec time(s) : The total time took by all the statements (in seconds)." + "\n" + \
                    "Longest Run(s)     : The longest run by the statement from all the execution (in seconds)" + "\n" + \
                    "Shortest Run(s)    : The longest run by the statement from all the execution (in seconds)" + "\n" + \
                    "Average Time(s)    : The average time taken in seconds ( i.e total execution time / number of execution )" + "\n\n"

        # Help documentation
        self.helpdoc = "OPTIONS:\n\n" \
          "-f, --hostmap-file=HOSTMAP FILE NAME           Hostmap file name\n" \
          "-s, --start-time=\"TIMESTAMP\"                   Start timestamp of the dump (FORMAT: YYYY-MM-DD HH:MI:SS)\n" \
          "-e, --end-time=\"TIMESTAMP\"                     End timestamp of the dump   (FORMAT: YYYY-MM-DD HH:MI:SS)\n" \
          "-b, --build-hostmap=\"DATE1[,DATE2,...]\"        Dates to search for logfile (FORMAT: YYYY-MM-DD)\n" \
          "-c, --contents=content1[,content2,...]         Contents of the segments interested (Default: ALL contents)\n" \
          "-v, --version                                  Display Version of the program \n" \
          "-d, --debug                                    Enable Debug Mode\n\n" \
          "EXAMPLE:\n\n" \
          "To build a hostmap with the logs from the date say \"2016-03-21\"\n\n" \
          "\t {0} -b \"2016-03-21\"\n\n" \
          "To build a hostmap with the logs from the date say \"2016-03-21\" & \"2016-03-22\" and content 1 & 2\n\n" \
          "\t {0} -b \"2016-03-21,2016-03-22\" -c 1,2\n\n" \
          "To execute the script with with the build hostmap from above\n\n" \
          "\t {0} -f hostmap -s \"2016-03-21 11:12:00\" -e \"2016-03-22 23:00:03\"\n\n" \
          "To enter into debug mode\n\n" \
          "\t {0} -f hostmap -s \"2016-03-21 11:12:00\" -e \"2016-03-22 23:00:03\" -d\n\n" \
          "COLUMN DESCRIPTION:\n\n".format(__file__) \
          + self.ColumnDescription + \
          "GENERAL INFORMATION:\n\n" \
          "-- The script need a connection to database to build the hostmap, make sure the database env is sourced & \"psql -d template1\" works\n" \
          "-- Ensure that log_duration GUC is turned ON for all the segments \n" \
          "-- If there are files (ends with .log) in the working directory make sure its moved or renamed to avoid conflict \n" \
          "-- Arguments -b & -c cannot run along with -f,-s,-e\n" \
          "-- Make sure the clock of segments servers are in sync\n" \
          "-- Script only gets the segment content information that are current primaries when the script is called\n"


# Store all those variables on the variables,
# so its easy to understand from where its coming from.
globalVariable = VariableClass()

# The logger process to log information of the screen
logger = globalVariable.logger


# Function: Usage(text)
# This function is used by the arguments checker
# It basically prints the Usage of the script and exit the script when called.
def Usage(text):

    print "\nUSAGE:"
    print (
        "{0} "
        "-f [--hostmap-file] "
        "-s [--start-time] "
        "-e [--end-time] "
        "-b [--build-hostmap] "
        "-c [--contents]  "
        "-d [--debug] "
        "-h [--help] \n".format(
                __file__
        )
    )
    print text
    sys.exit(2)


# Function: validate(date_text, date_format)
# Check the format of data specified in the argument, exits if not in the correct format.
def validate(date_text, date_format):

    try:
        datetime.strptime(
                date_text,
                date_format
        )

    except ValueError:
        if date_format == globalVariable.hostmapdate_format:
            text = "ERROR: Incorrect data format for -b , should be \"YYYY-MM-DD\""
            Usage(text)
        else:
            text = "ERROR: Incorrect data format for -s & -e, should be \"YYYY-MM-DD HH:MI:SS\""
            Usage(text)


# Function: hostmapWriter(file, data)
# This build the hostmap file
# When called it writes information to the file provided.
def hostmapWriter(file, data):

    logger.debug("Received information to write to hostmap file: \"{0}\"".format(
        file
    ))

    # Open the file provided
    try:
        fo = open(file, "a+")

    # Incase we don't have permission to write to the directory or some other
    # error when we try to open the file exit the program
    except IOError:
        logger.error("Unable to create the hostmap file in the working directory")
        sys.exit(2)

    # write the content and close the file
    with fo as output:
        output.write(data + "\n")
    fo.close()


# Function: LogFileWriter(text, dbid, host)
# This function when called write the information on the log
# Since the script is run from tempdir folder we dont need to provide the location.
def LogFileWriter(text, dbid, host):

    # Local Variable
    tempdir = globalVariable.tempdir
    outputfile = dbid + "_" + host + "_" + __file__ + ".log"

    # Lets try to open the file
    try:
        fo = open(outputfile, "a+")

    # If we receive any exception during the opening of file
    # Lets error out
    except IOError:
        logger.error("Unable to create the outputfile file: \"{0}\" in the directory: \"{1}\"".format(
            outputfile,
            tempdir
        ))
        sys.exit(2)

    # write the data
    with fo as output:
        output.write(text)
    fo.close()


# Function : jsonWriter(appenddata, jsondatafile)
# This writer process will keep appending the data from
# copy collector to a file in JSONformat and it will keep
# appending until we finish all the collection on the segment host
def jsonWriter(appenddata, jsondatafile):

    logger.debug("Received information to write the jsondata to file: \"{0}\"".format(
        jsondatafile
    ))

    # First lets open the file if exists
    try:
        with open(jsondatafile) as f:

            # And read all the data from the json file
            data = json.load(f)

            # And then update the new data we just received
            data.update(appenddata)

    # If its the first time then the file would not exists and we will end
    # up with the error, here we ensure that we use that error to create the
    # the file and add the content
    except IOError:
        data = appenddata

    # Right, so now we have the data lets write them up to the file
    with open(jsondatafile, 'w') as outfile:
        json.dump(data, outfile, indent=4)


# Function: RemoveTempdir(host, tempdir)
# This function removes the temp work directory on all the host
# of the hostmap.
def RemoveTempdir(host, tempdir):

    logger.debug("Removing old work directory: \"{0}\" if exists on host: \"{1}\"".format(
        tempdir,
        host
    ))

    # Check if there temp directory already on the host, if yes remove them
    try:
        subprocess.check_call(
                'ssh -qtt %s \"if [ -d %s ]; then rm -rf %s; fi\"' %
                (
                    host,
                    tempdir,
                    tempdir
                ),
                shell=True
        )

    # Error out on case we receive any error like permission issue etc..
    except subprocess.CalledProcessError, e:
        err = 'Error when trying to remove the old directory from %s:%s' % (host, tempdir)
        print >> sys.stderr, err
        sys.exit(1)


# Function: CreateTempdir(host)
# This function creates a temp work directory on all the host
# of the hostmap.
def CreateTempdir(host):

    # Local Variables.
    tempdir = globalVariable.tempdir

    logger.debug("Received information to build working directory: \"{0}\" on host: \"{1}\"".format(
        tempdir,
        host
    ))

    # Lets remove any pre-created directory so that we start in fresh
    # This step is great if we have created some files earlier and the script
    # didn't clean it up due to the failure of the script.
    # if we don't clean it up then there would be duplicate data
    RemoveTempdir(
            host,
            tempdir
    )

    logger.debug("Creating work directory: \"{0}\" on host: \"{1}\"".format(
        tempdir,
        host
    ))

    # Since we have removed the directory we will need to create it.
    # The point of the blank file (with dbid=0) is to ensure the scp doesn't fail since the
    # segment didn't provide any data , this file does not harm since there is no data.
    try:
        subprocess.check_call(
                'ssh -qtt %s \"mkdir -p %s; touch %s/0_%s_%s.log\"' %
                (
                    host,
                    tempdir,
                    tempdir,
                    host,
                    __file__
                ),
                shell=True
        )

    # Error if we encounter permission issue or such during creation
    except subprocess.CalledProcessError, e:
        err = 'Error when trying to remove the old files from %s:%s' % (host, tempdir)
        print >> sys.stderr, err
        sys.exit(1)


# Function : OutputFileMerger()
# This function merge all the output file from all the segments to
# a single file.
def OutputFileMerger():

    logger.info("Merging all the summary output from all segments onto a single file")

    # Local Variables
    WrkDir = os.path.dirname(os.path.realpath(__file__))
    OutputFileName = globalVariable.OutputFile
    files = "*{0}.log".format(__file__)
    read_files = glob.glob(files)
    ids = []
    hosts = []

    # Remove the zero bytes files
    for file in read_files:
        id = int(file.split("_")[0])
        if id == 0:
            os.remove(file)

    # Get the DBID's and the hostname from the filename
    for file in read_files:
        ids.append(int(file.split("_")[0]))
        hosts.append(file.split("_")[1])
        hosts = list(set(hosts))
        ids = list(set(ids))

    # Sort the ids
    ids.sort(key=int)

    # Get the Master logfile.
    # The first content of the merged output file
    # should be from the master, so we begin by first reading and merging the master contents
    if ids[1] == 1:
        logger.debug("Merging the contents of master segment(dbid): \"{0}\"".format(
            ids[0]
        ))
        file = "{0}/1_*_{1}.log".format(WrkDir, __file__)
        read_files = glob.glob(file)
        with open(OutputFileName, "a") as outfile:
            for f in read_files:
                        with open(f, "rb") as infile:
                            MasterLogHeading = globalVariable.MasterLogHeading.format('') + "\n\n"
                            outfile.write(MasterLogHeading)
                            outfile.write(infile.read())

        # now lets remove the master from the list since its already written
        ids.remove(1)

    # For the rest of the files lets
    for host in hosts:

        # This variable triggers when we have a new host
        # this helps in having heading per host
        stopper = 0

        for id in ids:
            files = "{0}/{1}_{2}_{3}.log".format(WrkDir, id, host, __file__)
            logger.debug("Merging the contents of segments(dbid/host): \"{0}/{1}\"".format(
                    id,
                    host
            ))
            read_files = glob.glob(files)

            # Append the outfile.
            with open(OutputFileName, "a") as outfile:

                # Read the file one by one
                for f in read_files:
                    with open(f, "rb") as infile:

                        # if this the first time we are writing from the host
                        # Lets place the header and place a stopper to avoid print
                        # until we reach next host
                        if stopper == 0:
                            SegmentLogHeading = "\n\n" + globalVariable.SegmentLogHeading.format('', host) + "\n\n"
                            outfile.write(SegmentLogHeading)
                            stopper = 1

                        # Write the contents from segments
                        outfile.write(infile.read())

    # Return the output file name.
    return OutputFileName


# Function : InputFileMerger(path, file, StartTime, EndTime)
# The idea behind this function is to merge all the content into a single file
# This helps in reducing the complexity of the code, we choose the same path as the logfile
# Since we assume the directory were the logfiles are should have enough space to accommodate this file.
def InputFileMerger(path, file, StartTime, EndTime):

    # Local variable
    # The reason why we need this junk is if file has no ending (i.e ends with "statement: "
    # after we create the inputfile , The script fails so we always ensure that
    # there is a ending line with this junk data
    junk = r'2016-04-16 12:10:47.750086 PDT,"gpadmin","template1",p100,th-1335258640,"127.0.0.1","7133",2016-04-16 ' \
           r'12:10:47 PDT,44752,con540,cmd27,seg-1,,,x44752,sx1,"LOG","00000","statement:",,,,,,,,,,," ' \
           r'SELECT typname, typlen FROM pg_type WHERE oid=19",,,,,,"SELECT typname, typlen FROM pg_type ' \
           r'WHERE oid=19",0,,"postgres.c",1618,'

    logger.debug("Received call to filter the logfile: \"{0}\" from \"{1}\" to \"{2}\"".format(
            file,
            StartTime,
            EndTime
            ))

    # Get the input file + path
    InputFile = path + "/" + globalVariable.InputFile

    # Read the content of the logfile and append only those rows of data
    # that starts and ends with the start time and end time respectively
    # we could have put in duration to reduce the size of the file even
    # further, but if we do that we will loose the information to get the
    # dump file name and we can't report the dump file size.
    read_files = glob.glob(file)
    with open(InputFile, "a") as outfile:
        for f in read_files:
            with open(f, "rb") as infile:
                    for row in infile.readlines():
                        if row[0:19] >= StartTime and row[0:19] <= EndTime:

                            # There is a issue when the SQL statement is on the next line
                            # the script fails since there is no ending, so we place rest of the terminator to avoid
                            # IndexError related errors.
                            # Moreover we don't care for the statement line in the script since the calculation are done
                            # on the duration line, so it doesnt matter for the remaining scripts
                            row = row.replace('statement: \n','statement:",,,,,,,,,,,",\n')
                            outfile.write(row)

                    # # End the inputfile with a junk data, so that we ensure always there is a ending..
                    outfile.write(junk)

    # Return the name of the inputfile which has the content
    return InputFile


# Function: HostmapStrip(hostmap)
# This function helps in splitting the hostmap file into list when called
# and then return the list to the caller so that it can work on the list.
def StripHostmap(hostmap):

    logger.debug("Reading and splitting the data in the hostmap file: \"{0}\"".format(
        hostmap
    ))

    # Open the hostmap file and the strip the data into list
    try:
        with open(hostmap, 'r') as f:
            return map(lambda x: tuple(x.rstrip().split(':')), f)

    # If we can't read the hostmap file error out like for eg. permission issue
    except IOError:
        logger.error("Can't read the hostmap file: \"{0}\" found at location: \"{1}\"".format(
                hostmap,
                os.getcwd()
        ))
        sys.exit(2)


# Function: ddboost_dump_size(dumpfile)
# Get the size of the dump using the gpmfr command.
def ddboost_dump_size(dumpfile):

    # Timestamp of the dump
    timestamp = dumpfile.split('gp_dump')[1].split('_')[3].split('.')[0]
    logger.debug("Timestamp of the backup: \"{0}\"".format(timestamp))

    # Command to extract the dump size
    command = 'gpmfr --list-file ' + timestamp + '| grep \'^' + dumpfile + '$\''

    # Execute the command
    sizecommand = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
    )

    # The size of the dump is
    try:
        size = sizecommand.stdout.read().split("\t")[3].split("\n")

    # If there is a exception
    except IndexError:
        logger.warn("Received exception in getting the size of the dump, setting the size of the dump to Zero")
        size = 0

    # return the size
    return size


# Function: DumpSize(dumplocation)
# Function to get the size of the dump
def DumpSize(dumplocation):

    # The size of the dump is
    try:
        size = os.path.getsize(dumplocation)

    # If there is a exception
    except OSError:
        logger.warn("Received exception in getting the size of the dump, setting the size of the dump to Zero")
        size = 0

    # return the size
    return size


# Function: HostmapBuilder(logdates, content)
# The below function builds the hostmap and write in the same directory from where the script is called
def HostmapBuilder(logdates, content):

    # Local Variables.
    query = globalVariable.query
    HealthCheckquery = globalVariable.HealthCheckquery
    hostmapfile = globalVariable.hostmapfile

    # Query add-ons
    contentquery = " AND content in (" + str(content) + ")"
    orderbycluase = " ORDER BY hostname"

    # Split the provided dates
    logdates = logdates.split(',')

    # Incase user repeats the dates, lets merge them as one
    # else we will be processing duplicate data
    logdates = list(set(logdates))

    # Validate the dates are in the correct format so that its a valid search
    for dates in logdates:
        validate(
                dates,
                globalVariable.hostmapdate_format
        )

    # Check of the database can be accessed
    logger.info("Checking if the database is available")
    OSreturnCode = os.system(
            HealthCheckquery
    )

    # If the database is not started then error out and exit the program
    if OSreturnCode != 0:
        logger.error("Database template1 seems to be unreachabe, please check "
                     "the environment is sourced or database is up")
        sys.exit(2)

    # If the contents are provided change the default query.
    if content != None:
        logger.info("Changing the query to incorporate the content information")
        query = query + contentquery

    # Get the segment information from the database
    # We assume this step works since the healthcheck of the database has passed.
    getSegInfoCommand = "psql -d template1 -Atc \"" + query + orderbycluase + "\""

    # Read the segment information provided above and split the content
    SegInfo = os.popen(getSegInfoCommand)
    SegInfoReader = SegInfo.read().split('\n')

    # Delete the last line, since its null always.
    del SegInfoReader[-1]

    # This is ensure we have some data to build a hostmap, if the user provided content doesnt exists
    # then its better to exit.
    if not SegInfoReader:
        logging.error("No segment information found from the database, the list is empty")
        sys.exit(2)

    # If there is a hostmap file then lets remove it from the directory
    # else we will keeping appending and the script might bring in duplicate information.
    if os.path.exists(hostmapfile):
        try:
            os.remove(hostmapfile)

        # If there is permission issue in removing or some other reason error out
        except OSError:
            logger.error("Unable to remove the exiting hostmap file in the current directory")
            sys.exit(2)

    # Loop with the segment information obtained
    logger.debug("Attempting to generate the hostmap file")
    for segmap in SegInfoReader:

        # Split the content into a list
        InfoSeg = segmap.split(':')
        mergeResult = []
        temp = []

        # sub loop to hunt for the logs with those dates provided
        for file in logdates:
            logger.debug("Searching for the file with: \"{0}\" on host: \"{1}\" at location: \"{2}\"".format(
                    file,
                    InfoSeg[0],
                    InfoSeg[1]
            ))
            sshcmd = "ssh " + InfoSeg[0] + " \"find " + InfoSeg[1] + " -name *" + file + "*\""
            logs = subprocess.Popen(
                    sshcmd,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
            )
            result = logs.stdout.read().split("\n")
            temp = result
            mergeResult = mergeResult + temp
            del mergeResult[-1]

            # If we don't find any logs produce a warning message to inform the user.
            if not mergeResult:
                logger.warn("Didn't find any logs for date: \"{0}\" on host: \"{1}\" location: {2}".format(
                    file,
                    InfoSeg[0],
                    InfoSeg[1]
                ))

        # Once obtained the logfile join them via a commaa
        logfile = ",".join(mergeResult)

        # From the provided dates, if we cant find any logs with any of the dates
        # error out and exit, since there is no use of running the script with the hostmap
        # since there is no file to hunt for information.
        if not logfile:
            logger.error("No file found with the dates provided at host: \"{0}\", location: \"{1}\"".format(
                InfoSeg[0],
                InfoSeg[1]
            ))
            sys.exit(2)

        # Write them to a file called hostmap
        HostmapInfo = InfoSeg[0] + ":" + logfile + ":" + InfoSeg[2] + ":" + InfoSeg[3]
        hostmapWriter(
                hostmapfile,
                HostmapInfo
        )

    # Provide the location where the hostmap is created.
    current_directory = os.getcwd()
    logger.info("Hostmap file \"{0}\" has been generated, Location of the hostmap file: \"{1}\"".format(
            hostmapfile,
            current_directory
    ))

    # Exit the program
    sys.exit(0)


# Function: ArgumentParser(argv)
# This function parses the arguments supplied via the script
# and check if everything is alright.
def ArgumentParser(argv):

    # Local Variables.
    version = globalVariable.version
    StartTime = globalVariable.StartTime
    EndTime = globalVariable.EndTime
    filename = globalVariable.filename
    formats = globalVariable.loggerformats
    contents = globalVariable.contents
    hostmapdates = globalVariable.hostmapdates
    debug = globalVariable.debug
    logger = globalVariable.logger

    # Try to get the options passed
    try:
        opts, args = getopt.getopt(
                argv,
                'hf:s:e:b:c:d:v',
                [
                    'help',
                    'hostmap-file=',
                    'start-time=',
                    'end-time=',
                    'build-hostmap=',
                    'contents='
                    'debug',
                    'version'
                ]
        )
        if not opts:
            text = "No Option Specified:"
            Usage(text)

    # If those are invalid option, error out.
    except getopt.GetoptError, e:
        Usage(e)

    # Check what are the options passed and assign the right arguments
    for opt, arg in opts:

        if opt in ('-h', '--help'):
            text = globalVariable.helpdoc
            Usage(text)

        elif opt in ('-v', '--version'):
            print "Program {0} version: {1}".format(__file__, version)
            sys.exit(0)

        elif opt in ('-f', '--hostmap-file'):

            # Check if the hostmap exists
            if not os.path.exists(arg):
                text = "ERROR: file \"" + arg + "\" does not exists"
                Usage(text)

            # Check if the hostmap is file or other format (like directory,binary etc)
            if not os.path.isfile(arg):
                text = "ERROR: \"" + arg + "\" doesn't look like a file"
                Usage(text)
            filename = arg

        elif opt in ('-s', '--start-time'):
            StartTime = arg
            validate(
                    StartTime,
                    globalVariable.StartEnddate_format
            )

        elif opt in ('-e', '--end-time'):
            EndTime = arg
            validate(
                    EndTime,
                    globalVariable.StartEnddate_format
            )

        elif opt in ('-b', '--build-hostmap'):
            hostmapdates = arg

        elif opt in ('-c', '--contents'):
            contents = arg

        elif opt in ('-d', '--debug'):
            debug = 1
            logging.basicConfig(
                    format=formats,
                    level=logging.DEBUG
            )

        else:
            text = "ERROR: Unknown Options"
            Usage(text)

    logging.basicConfig(
            format=formats,
            level=logging.INFO
    )

    # Parse check
    logger.info("Starting the program: {0}".format(__file__))

    # -b & -c cannot run along with -f,-s.-e
    # Since -b is to build a hostmap, so we cant run before building one.
    if hostmapdates and (filename or StartTime or EndTime):
        text = "ERROR: -b cannot work along with arguments -f, -s, -e"
        Usage(text)

    # Content cannot be used when the hostmap is created, you will use it when
    # building hostmap.
    elif contents and (filename or StartTime or EndTime):
        text = "ERROR: -c cannot work along with arguments -f, -s, -e"
        Usage(text)

    # If there is -b and -c pass on the contents provided by the parser.
    elif hostmapdates and contents:
        HostmapBuilder(hostmapdates, contents)

    # is no content then pass on the default that is None
    elif hostmapdates and not contents:
        HostmapBuilder(hostmapdates, contents)

    # if only -c is passed then error out, since we cant hunt for all logs
    # as that is cause a lot of time and maybe error prone.
    elif contents and not hostmapdates:
        text = "ERROR: -c parameter cannot run as a single entity, need -b option"
        Usage(text)

    # if you have a start time then there should be a end time.
    elif not StartTime and EndTime:
        text = "ERROR: End Time needs a Start time."
        Usage(text)

    # if there is a end time then there should be a start time.
    elif StartTime and not EndTime:
        text = "ERROR: Start Time needs a End time."
        Usage(text)

    # if end time is less or equal to start time it should error out.
    elif StartTime >= EndTime:
        text = "ERROR: End Time cannot be greater or equal to Start Time"
        Usage(text)

    # If there is no mandatory parameter passed, error out.
    # The reason why start time and end time has mandatory is because
    # If there was multiple runs of backup on the same day then the script
    # would pull data for all of them which basically means the output log
    # would be very confusing to read, so lets ask the user what was the start time of
    # the backup they are interested and when did it end,
    # so that we gather information for only those time.
    elif not filename or not StartTime or not EndTime:
        text = "ERROR: -f,-s & -e parameters are mandatory for the script to run."
        Usage(text)

    # Return the mandatory parameter to be used by the rest of the script.
    return filename, StartTime, EndTime, debug


# Function: parseHostfile(hostmap)
# This function basic function is to ensure is to split the hostmap based on host
# so that we can send that hostmap to the respective host and then the host run the program
# on all the logfiles found on the segment running on that host
def parseHostfile(hostmap):

    logger.info("The function to parse the hostmap file: \"{0}\" has been called".format(
            hostmap
    ))

    # Local Variables.
    hosts = []
    tempdir = globalVariable.tempdir

    # Strip the hostmap into list
    logger.info("Reading the contents of the hostmap: \"{0}\"".format(
        hostmap
    ))
    strippedhostmap = StripHostmap(hostmap)

    # In case if the hostmap is created manually or modified or
    # for some other reason hostmap builder didn't put in necessary
    # contents lets check and error out now rather than script providing
    # misleading error message or error prone data.
    logger.info("Checking if the contents in the hostmap: \"{0}\" are valid".format(
        hostmap
    ))
    for line in strippedhostmap:

        # Assign the variable based on what it is
        host = line[0]
        logfile = line[1]
        dbid = line[2]
        content = line[3]

        # If there is anything missing in the script, it should not run we will exit with an error.
        # If there is no host
        if not host:
            logger.error("There is no host information for dbid: \"{0}\", content: \"{1}\"".format(
                dbid,
                content
            ))
            sys.exit(2)

        # if there is no logfile
        if not logfile:
            logger.error("There is no logfile information for host: \"{0}\",  dbid: \"{1}\", content: \"{2}\"".format(
                host,
                dbid,
                content
            ))
            sys.exit(2)

        # If there is no dbid
        if not dbid:
            logger.error("There is no dbid information for host: \"{0}\", content: \"{1}\"".format(
                host,
                content
            ))
            sys.exit(2)

        # If there is no content
        if not content:
            logger.error("There is no content information for host: \"{0}\",  dbid: \"{1}\"".format(
                host,
                dbid
            ))
            sys.exit(2)

        hosts.append(line[0])

    # The hosts created above will have lots of duplicates
    # the below ensure its is set to unique.
    hosts = list(set(hosts))

    # Time to create work directory
    logger.info("Creating work directory and splitting the hostmap by host")
    for h in hosts:

        # For each host create a working directory
        CreateTempdir(h)

    # Lets start the program by calling the contents in the hostmap one by one
    # here we split the hostmap based on host and then send write it to temp
    # work directory
    for h in hosts:

        # Create a hostmap file by hostname
        hostmapperfile = tempdir + "/hostmap_" + h

        # Using the regular expression (re) module lets separate the hostmap
        # by host and then create separate hostmap by host
        # The reason why we add ":" with hostname to differentate b/w sdw1, sdw11, sdw12 etc
        h = h + ':'
        f = open(hostmap)
        for line in f:
            line = line.rstrip()
            if re.search(h, line):
                hostmapWriter(
                        hostmapperfile,
                        line
                )

    # Return the list of host obtained to the main program to launch the process.
    return hosts


# Function: SQLOutputFormatter(summary, basicinfo, segInfo)
# This function format the SQL query into a readable log
def SQLOutputFormatter(summary, basicinfo, segInfo):

    # Local Variables
    fmt1 = globalVariable.SQLfmt1
    fmt2 = globalVariable.SQLfmt2
    fmt3 = globalVariable.SQLfmt3
    fmt4 = globalVariable.SQLfmt4
    date_format = globalVariable.date_format
    dbid = segInfo['dbid']
    host = segInfo['host']
    timeformat = globalVariable.timeFormat
    timeconvertor = globalVariable.timeConvertor
    sizeformat = globalVariable.sizeFormat

    # If the call of the function is from exclusive lock, then lets capture the information of the
    # the exclusive process
    if basicinfo['mode'] == 'ExclusiveLock':

        logger.info("Formatting the data obtained for exclusive lock process "
                    "for segment (host/dbid/content): \"{0}/{1}/{2}\"".format(
            segInfo['host'],
            segInfo['dbid'],
            segInfo['content']
        ))

        dbInformation = 'Segment Information (host/dbid/content)                                         : {0}'.format(segInfo['host'] + " / " + segInfo['dbid'] + " / " + segInfo['content'])
        expid = 'Exclusive Lock requesting PID                                                   : {0}'.format(basicinfo['ExclusiveLockPid'])
        getpgclassLock = 'Lock on pg_class requested at                                                   : {0}'.format(basicinfo['timeforlock'])
        releasepgclassLock = 'Lock on pg_class released at                                                    : {0}'.format(basicinfo['timeforrelease'])
        sizeofdump = 'Size of the Master dump(' + sizeformat + ')                                                     : {0}'        .format(basicinfo['dumpsize'])
        sizeofpostdump = 'Size of the Master post dump(' + sizeformat + ')                                                : {0}'        .format(basicinfo['postdumpsize'])
        totalqueryexecuted = 'Total query executed by the pid                                                 : {0}'.format(basicinfo['totalexecution'])
        totaltimespend = 'Total time(' + timeformat + ') spend by the PID in database for running all the metadata queries : {0:.2f}'.format(basicinfo['totaltime'] * timeconvertor)
        timespendonpgclass = 'Total time spend holding pg_class Lock                                          : {0}'.format(
            datetime.strptime(
                    basicinfo['timeforrelease'][:-4],
                    date_format
            ) - datetime.strptime(
                    basicinfo['timeforlock'][:-4],
                    date_format
            )
        )

        # Call the LogFile writer and write the information on the log.
        LogFileWriter(
            "\n" + dbInformation +
            "\n" + expid +
            "\n" + getpgclassLock +
            "\n" + releasepgclassLock +
            "\n" + timespendonpgclass +
            "\n" + sizeofdump +
            "\n" + sizeofpostdump +
            "\n" + totalqueryexecuted +
            "\n" + totaltimespend +
            "\n" + "Summary of queries run under that PID :" + "\n\n",
                dbid,
                host
        )

    # If the call of the function is from Share lock, then lets capture the information of the
    # the share lock process
    elif basicinfo['mode'] == 'ShareLock':

        logger.info("Formatting the data obtained for Share lock process "
                    "for segment (host/dbid/content): \"{0}/{1}/{2}\"".format(
            segInfo['host'],
            segInfo['dbid'],
            segInfo['content']
        ))

        dbInformation = 'Segment Information (host/dbid/content)                                         : {0}'.format(segInfo['host'] + " / " + segInfo['dbid'] + " / " + segInfo['content'])
        shpid = 'Share lock requesting PID                                                       : {0}'.format(basicinfo['ShareLockPid'])
        totalqueryexecuted = 'Total query executed by the pid                                                 : {0}'.format(basicinfo['totalexecution'])
        totaltimespend = 'Total time(' + timeformat + ') spend by the PID in database for running all the metadata queries : {0:.2f}'.format(basicinfo['totaltime'] * timeconvertor)

        # Call the LogFile writer and write the information on the log.
        LogFileWriter(
            "\n" + dbInformation +
            "\n" + shpid +
            "\n" + totalqueryexecuted +
            "\n" + totaltimespend +
            "\n" + "Summary of queries run under that PID :" +
            "\n\n",
            dbid,
            host
        )

    # If the call of the function is from segments process, then lets capture the information of the
    # the segment process
    elif basicinfo['mode'] == 'Segment Process':

        logger.info("Formatting the data obtained for Segment process "
                    "for segment (host/dbid/content): \"{0}/{1}/{2}\"".format(
            segInfo['host'],
            segInfo['dbid'],
            segInfo['content']
        ))

        dbInformation = 'Segment Information (host/dbid/content)                                         : {0}'.format(segInfo['host'] + " / " + segInfo['dbid'] + " / " + segInfo['content'])
        shpid = 'Segment Process PID                                                             : {0}'.format(basicinfo['SegmentPid'])
        totalqueryexecuted = 'Total query executed by the pid                                                 : {0}'.format(basicinfo['totalexecution'])
        totaltimespend = 'Total time(' + timeformat + ') spend by the PID in database for running all the metadata queries : {0:.2f}'.format(basicinfo['totaltime'] * timeconvertor)

        # Call the LogFile writer and write the information on the log.
        LogFileWriter(
            "\n" + dbInformation +
            "\n" + shpid +
            "\n" + totalqueryexecuted +
            "\n" + totaltimespend +
            "\n" + "Summary of queries run under that PID :" +
            "\n\n",
            dbid,
            host
        )

    # Line breaker
    LineAdder = fmt1.format('')

    # Heading information
    Header = fmt2.format(
             'Statement',
             'First Run',
             'Last Run',
             '# of Execution',
             'Total Exec time(' + timeformat + ')',
             'Longest Run(' + timeformat + ')',
             'Shortest Run(' + timeformat + ')',
             'Average Time(' + timeformat + ')'
    )

    # Write the heading information onto the logfile
    LogFileWriter(
            LineAdder + "\n" + Header + "\n" + LineAdder + "\n",
            dbid,
            host
     )

    # Write the SQL information on the log.
    for items in sorted(summary, key=itemgetter('firstdate')):
         LogFileWriter(fmt3.
             format(
                 items['statement'],
                 items['firstdate'],
                 items['enddate'],
                 items['execution'],
                 items['totalduration'] * timeconvertor,
                 items['maxduration'] * timeconvertor,
                 items['minduration']* timeconvertor,
                 (items['totalduration']/items['execution']) * timeconvertor
         ) + "\n",
                       dbid,
                       host
    )

    # Add the line breaker
    LogFileWriter(
            LineAdder + "\n",
            dbid,
            host
     )

    # Add the total time taken by the sql to the logfile
    LogFileWriter(
        fmt4.format('Total', '', '', basicinfo['totalexecution'], basicinfo['totaltime'] * timeconvertor, '', '', '') +
        "\n" +
        LineAdder + "\n",
            dbid,
            host
    )


# Function : CopyOutputFormatter(jsondatafile, host)
# Once we get the merged data we then format the data and
# write onto the file
def CopyOutputFormatter(jsondatafile, host):

    # Local Variables
    Heading = []
    data = {}
    TableList = {}
    dumpsize = []
    totalduration = []
    totaldurationpertable = {}
    TotalofTotal = globalVariable.TotalofTotal
    Stopper = globalVariable.Stopper
    timeformat = globalVariable.timeFormat

    # Copy formats
    fmt1 = globalVariable.Copyfmt1
    fmt2 = globalVariable.Copyfmt2
    fmt3 = globalVariable.Copyfmt3

    # Lets read all then contents from the json file
    with open(jsondatafile) as file:
        data = json.load(file)

    # Lets have the heading information on the list
    # and store the addon information on a another list and
    # delete from the data
    for key in data:
        Heading.append(key)

        # If the copy json file has issues, lets do a early exit from this function
        # rather than providing error output especially this issue can happen when there is incremental backup and
        # no changes was found.
        try:
            totalduration.append(data[key]['InfoAddonstmts']['TotalstmtDuration'])
            dumpsize.append(data[key]['InfoAddonstmts']['SegDumpsize'])
            del data[key]['InfoAddonstmts']
        except KeyError:
            logger.warn("Unable to format the time from data backup, check the start / end date provided")
            return "Nothing"

    # Get the length of the heading list to update the format string
    ListLength = len(Heading) + 1

    # Time to extend the fmt string based on the length of the heading
    while ListLength != 1:
        fmt1 = fmt1 + '{' + str(ListLength) + ':>21}|'
        fmt2 = fmt2 + '{0:->21}|'
        fmt3 = fmt3 + '{' + str(ListLength) + ':>21}|'
        ListLength -= 1

    # This is important in case we have catalog issues, such as
    # some table exists on the segments and not on segments
    # the formatting would fail with IndexError, so to overcome this
    # we make a unique list of all the table for all the segments in the host
    # and use as base to compare the tables.
    for h in Heading:
        for key in data[h]:
            if key not in TableList:
                TableList[key] = []
                totaldurationpertable[key] = 0

    # Now we have a base to check on table names , lets see who has the
    # data and who doesnt and store them on a list, same time lets add
    # the time per table.
    for h in Heading:
        for table in TableList:
            if table in data[h]:
                TableList[table].append(data[h][table])
                totaldurationpertable[table] += data[h][table]
            else:
                TableList[table].append(None)

    # Okie now we have the data, its time to format and write data
    # Lets give the dbid of the copy file 9999999 , since this ensure the file always read by the
    # copy formatter at the end and the file is merged at the end of the SQL output
    if Stopper != 1:
        CopyDataLine = "\n" + "Data Backup Time Table("+ timeformat +") for host: {0}".format(host) + "\n"
        LogFileWriter(
            CopyDataLine,
            '9999999',
            host
    )

    # Line adder.
    LineAdder = fmt2.format('')

    # Heading Line
    HeadingLine = "\n" + LineAdder + "\n" + fmt1.format(
                     'Table Name',
                     'Total',
                     *Heading
             ) + "\n" + LineAdder + "\n"

    # Writing to the file.
    LogFileWriter(
        HeadingLine,
        '9999999',
        host
    )

    # The table duration time, we sort based on the name.
    for table in sorted(TableList):
        TableDurationLine = fmt3.format(
                 table,
                 totaldurationpertable[table],
                 *TableList[table]
         ) + "\n"

        # Writing to the file.
        LogFileWriter(
                TableDurationLine,
                '9999999',
                host
        )

        # Sum of all the total.
        TotalofTotal = totaldurationpertable[table] + TotalofTotal

    # Total time took
    TotalLine = LineAdder + "\n" + fmt3.format(
            'Total Time Spend(s)',
            TotalofTotal,
            *totalduration
         ) + "\n" + LineAdder + "\n"

    # Writing to the file.
    LogFileWriter(
            TotalLine,
            '9999999',
            host
        )

    # Size of the dump
    DumpSizeLine = fmt3.format('Size of Dump(MB)', sum(dumpsize), *dumpsize) + "\n" + LineAdder + "\n\n"

    # Writing to the file.
    LogFileWriter(
            DumpSizeLine,
            '9999999',
            host
        )


# Function: MasterLogReader(logfile, segInfo)
# This is the main function that reads the master log, this obtain the pid of the backup process
# and gathers the statement, number of execution and the time it took to execute
def MasterLogReader(logfile, segInfo):

    # Local Variable

    # The below parameters tells the Master log reader where you will find
    # date , pid , duration it took & the statement etc
    pDate = globalVariable.row_date
    pPid = globalVariable.row_pid
    pDuration = globalVariable.row_duration
    pQuery = globalVariable.row_query
    pDumpLocation = globalVariable.row_dumplocation
    sizeConvertor = globalVariable.sizeConvertor

    # Variables to store the information gathered.
    sharelockpid = []
    exclusivelockpid = []
    SummaryExclusiveLock = []
    SummaryShareLock = []

    # Basic information on the PID that stores that executed the Exclusive lock
    InfoExclusiveLock = {
        'mode': 'ExclusiveLock',
        'flag': 'NO DATA',
        'DBInfo': segInfo,
        'ExclusiveLockPid': 0,
        'timeforlock': '',
        'timeforrelease': '',
        'totalexecution': 0,
        'totaltime': 0,
        'dumpsize': 0,
        'postdumpsize': 0
    }

    # Basic information on the PID that stores that executed the Share lock
    InfoShareLock = {
        'mode': 'ShareLock',
        'flag': 'NO DATA',
        'DBInfo': segInfo,
        'ShareLockPid': 0,
        'totalexecution': 0,
        'totaltime': 0
    }

    # We read the master log aka Input file 3 times
    # first read to find the pid of the user that executed the pg_class lock and the share lock
    try:
        file = open(logfile, 'rb')

    # What if during this time the log was deleted by log_cleanup, since the file cannot be read
    # we will exit.
    except IOError:
        logger.error("Can't read the logfile: \"{0}\"".format(
                     logfile
        ))
        sys.exit(2)

    # Reading the file

    with file as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        logger.info("Reading the logfile to identify the exclusive lock PID & share lock PID")

        # Read the file row by row

        for row in reader:

            # While we capture the information of PID, lets get the dump file name and
            # location, so that we can get the size of the dump, since in the master has two dumps
            # we will use the exception clause to get the post data dump
            if row[pDumpLocation].startswith('gp_dump_agent command line'):

                logger.info("Getting the size of the dump for the segment (host/dbid/content): \"{0}/{1}/{2}\"".format(
                    segInfo['host'],
                    segInfo['dbid'],
                    segInfo['content']
                ))
                # Dump file line
                line = row[globalVariable.row_dumplocation]

                # If the line has ddboost command on it.
                if 'gpddboost' in line:

                    logger.debug("This a ddboost backup")

                    # fully qualified path
                    path = row[18].split('--to-file=')[1].split(" ")[0].rpartition('/')

                    # Filename of the dump.
                    dumplocation = path[-1]

                    # If file has postdata word
                    if 'post_data' in dumplocation:

                        # Get the file size
                        FileSize = ddboost_dump_size(dumplocation) / sizeConvertor
                        InfoExclusiveLock['postdumpsize'] = FileSize

                    # If doesnt have post data then its actual dump
                    else:
                        FileSize = ddboost_dump_size(dumplocation) / sizeConvertor
                        InfoExclusiveLock['dumpsize'] = FileSize

                    logger.debug("Backup stats (path|filename|size): \"{0}|{1}|{2}\"".format(
                        path,
                        dumplocation,
                        FileSize
                    ))

                # If the backup is not at ddboost
                else:

                    logger.debug("The backup is on the filesystem")

                    # Lets try reading the row which has the dump location information
                    try:
                        dumplocation = line.split('>')[2].strip()
                        FileSize = DumpSize(dumplocation) / sizeConvertor
                        InfoExclusiveLock['dumpsize'] = FileSize

                    # Here we do expect a error, so lets use the error detection to find the post data
                    # dump location and size.
                    except IndexError:
                        dumplocation = line.split('>')[1].strip()
                        FileSize = DumpSize(dumplocation) / sizeConvertor
                        InfoExclusiveLock['postdumpsize'] = FileSize

                    logger.debug("Backup stats (path|size): \"{0}|{1}\"".format(
                        dumplocation,
                        FileSize
                    ))

            # The only way to identify the backup pid is to hunt for pg_class lock
            # There is no other clear way as of the moment.
            # In normal condition database users would not run lock on pg_class but if they do
            # we may pick that process pid and gather the information, we can't put and check here
            # since there is no clear differentator.
            if row[pQuery] == "LOCK TABLE pg_catalog.pg_class IN EXCLUSIVE MODE;":
                exclusivelockpid.append(row[pPid])
                exclusivelockpid = list(set(exclusivelockpid))

            # During the same read of the logfile we will also hunt for the pid which executed the access share lock
            # again , if the database users run share lock via their application job we may end up having the wrong pid
            # there is nothing we can do but to print everything that was run by the user.
            if row[pQuery].startswith('LOCK TABLE') and row[pQuery].endswith('IN ACCESS SHARE MODE'):
                sharelockpid.append(row[pPid])
                sharelockpid = list(set(sharelockpid))

        logger.debug("Exclusive lock PID information obtained is: \"{0}\"".format(
                exclusivelockpid
                     ))

        logger.debug("Share lock PID information obtained is: \"{0}\"".format(
                exclusivelockpid
                     ))

        # Now if we provided the wrong dates (start time and end time) or no backup was run this can happen
        # that we get no information of the PID, lets warn and continue with the rest of the log or steps.
        if not exclusivelockpid:
            logger.warn("Didn't find any PID that executed the pg_class exclusive lock on the master log, "
                        "this may result in no information logged for the exclusive lock process")
        if not sharelockpid:
            logger.warn("Didn't find any PID that executed share lock on the master log, "
                        "this may result in no information logged for the shared lock process")

        # This is the second read of the log file
        # Now we have obtained the pid that executed the exclusive lock, lets gather information
        # about the statement, number of time it was executed, duration and some basic info etc...
        for expid in exclusivelockpid:

            logger.info("Reading the logfile to capture the information for all "
                        "things run by exclusive lock process: \"{0}\"".format(
                expid
            ))

            # Go to the start of the file again.
            file.seek(0)

            # Read data line by line
            for row in reader:

                # We are interested on the rows with that pid and that line that has the duration clause in it
                # rest we will skip.
                if row[pPid] == expid and row[pDuration].split(' ')[0] == "duration:":

                    # We found the statement lets gather information

                    # Lets disable the flag
                    InfoExclusiveLock['flag'] = 'DATA'

                    # Statement
                    statement = row[pQuery]

                    # Pid of the process.
                    InfoExclusiveLock['ExclusiveLockPid'] = expid

                    # Total query executed by the PID ( incrementing each time we find one row )
                    InfoExclusiveLock['totalexecution'] += 1

                    # Total time taken by the PID ( Summing all duration each time we find one row )
                    InfoExclusiveLock['totaltime'] += float(row[pDuration].split(' ')[1])

                    # When we receive the statement we dont have that information on the list
                    # So we make the first entry.
                    if not any(d['statement'] == statement for d in SummaryExclusiveLock):
                        SummaryExclusiveLock.append(
                                {
                                    'statement': statement,
                                    'execution': 1,
                                    'firstdate': row[pDate],
                                    'enddate': row[pDate],
                                    'totalduration': float(row[pDuration].split(' ')[1]),
                                    'maxduration': float(row[pDuration].split(' ')[1]),
                                    'minduration': float(row[pDuration].split(' ')[1])
                                 }
                        )

                    # Now if we receive the same statement we already have that on the list
                    else:
                        for d in SummaryExclusiveLock:
                            if d['statement'] == statement:

                                # this time we just increment
                                d['execution'] += 1

                                # We capture the end time
                                d['enddate'] = row[pDate]

                                # Sum the duration
                                d['totalduration'] += float(row[pDuration].split(' ')[1])

                                # try to find who has the highest execution time
                                if d['maxduration'] < float(row[pDuration].split(' ')[1]):
                                    d['maxduration'] = float(row[pDuration].split(' ')[1])

                                # try to find who has the lowest execution time
                                if d['minduration'] > float(row[pDuration].split(' ')[1]):
                                    d['minduration'] = float(row[pDuration].split(' ')[1])

                    # Lets get the timestamp when the pg_class lock was issued
                    if row[pQuery] == "LOCK TABLE pg_catalog.pg_class IN EXCLUSIVE MODE;":
                        InfoExclusiveLock['timeforlock'] = row[pDate]

                    # Let get the timestamp to when the pg_class lock was released
                    if row[pQuery] == "COMMIT":
                        InfoExclusiveLock['timeforrelease'] = row[pDate]

            # Reality Check, Lets check if we have data for the exclusive lock PID
            # If we do then lets call the SQL Formatter and print the information on the log
            if InfoExclusiveLock['ExclusiveLockPid'] != 0 and InfoExclusiveLock['flag'] == 'DATA':
                logger.debug("Calling Exclusive Lock SQL Formatter "
                             "for master segment (host/dbid/content): \"{0}/{1}/{2}\"".format(
                    segInfo['host'],
                    segInfo['dbid'],
                    segInfo['content']

                ))
                SQLOutputFormatter(
                        SummaryExclusiveLock,
                        InfoExclusiveLock,
                        segInfo
                )

            # If there is no DATA then write the information on the logs as NO DATA Available
            # and provide a warning message to the user and continue with the
            # rest of the steps.
            elif InfoExclusiveLock['flag'] == 'NO DATA':

                logger.warn("Obtained no information about the exclusive lock PID:\"{0}\", seems like"
                            " log_duration was not turned on or invalid start/end time specified".format(
                    exclusivelockpid
                ))

                text = "\n NO DATA found for the Exclusive lock process PID: \"{0}\" " \
                       "on segment (host/dbid/content): \"{1}/{2}/{3}\"" \
                       " Check on the Start Time and End Time" \
                       " or check on if the log_duration GUC is turned on".format(
                    exclusivelockpid,
                    segInfo['host'],
                    segInfo['dbid'],
                    segInfo['content']
                )

                LogFileWriter(
                    text,
                    segInfo['dbid'],
                    segInfo['host']
                )

        # This is the third and last read of the log file
        # Now we have obtained the pid that executed the share lock, lets gather information
        # about the statement, number of time it was executed, duration and some basic info etc...
        for shpid in sharelockpid:

            logger.info("Reading the logfile to capture the information for all things "
                        "run by share lock process: \"{0}\"".format(
                    shpid
            ))

            # Go to the start of the file again.
            file.seek(0)

            # Read data line by line
            for row in reader:

                # We are interested on the rows with that pid and that line that has the duration clause in it
                # rest we will skip.
                if row[pPid] == shpid and row[pDuration].split(' ')[0] == "duration:":

                    # We found the statement lets gather information

                    # Lets disable the flag
                    InfoShareLock['flag'] = 'DATA'

                    # Pid of the process.
                    InfoShareLock['ShareLockPid'] = shpid

                    # Total query executed by the PID ( incrementing each time we find one row )
                    InfoShareLock['totalexecution'] += 1

                    # Total time taken by the PID ( Summing all duration each time we find one row )
                    InfoShareLock['totaltime'] += float(row[pDuration].split(' ')[1])

                    # Statement
                    # We club all the LOCK TABLE as one statement
                    if row[pQuery].startswith('LOCK TABLE'):
                       statement="LOCK TABLE X IN ACCESS SHARE MODE"

                    # We club all the COPY statement as one statement
                    elif row[pQuery].startswith('COPY'):
                       statement="COPY X(x,y,z,..) TO stdout"

                    # We trim the statement into 28 character since after that some of the statement
                    # starts to add relation name which make that statement (even though the same) points as
                    # different statement.
                    elif row[pQuery].upper().startswith('SELECT'):
                       statement = row[pQuery][0:28].split("\n")[0]

                    # We club the search_path as one
                    elif row[pQuery].upper().startswith('SET SEARCH_PATH'):
                       statement = "SET SEARCH_PATH X, pg_catalog"

                    # Rest of the statement we print as it is.
                    else:
                       statement = row[pQuery]

                    # When we receive the statement we dont have that information on the list
                    # So we make the first entry.
                    if not any(d['statement'] == statement for d in SummaryShareLock):
                        SummaryShareLock.append(
                                {
                                    'statement' : statement,
                                    'execution' : 1,
                                    'firstdate' : row[pDate],
                                    'enddate' : row[pDate],
                                    'totalduration' : float(row[pDuration].split(' ')[1]),
                                    'maxduration' : float(row[pDuration].split(' ')[1]),
                                    'minduration' : float(row[pDuration].split(' ')[1])
                                 }
                         )

                    # Now if we receive the same statement we already have that on the list
                    else:
                        for d in SummaryShareLock:
                            if d['statement'] == statement:

                                # this time we just increment
                                d['execution'] += 1

                                # We capture the end time
                                d['enddate'] = row[pDate]

                                # Sum the duration
                                d['totalduration'] += float(row[pDuration].split(' ')[1])

                                # try to find who has the highest execution time
                                if d['maxduration'] < float(row[pDuration].split(' ')[1]):
                                    d['maxduration'] = float(row[pDuration].split(' ')[1])

                                # try to find who has the lowest execution time
                                if d['minduration'] > float(row[pDuration].split(' ')[1]):
                                    d['minduration'] = float(row[pDuration].split(' ')[1])

            # Reality Check, Lets check if we have data for the share lock PID
            # If we do then lets call the SQL Formatter and print the information on the log
            if InfoShareLock['ShareLockPid'] != 0 and InfoShareLock['flag'] == 'DATA':
                logger.debug("Calling Share Lock SQL Formatter for master segment (host/dbid/content): \"{0}/{1}/{2}\"".format(
                    segInfo['host'],
                    segInfo['dbid'],
                    segInfo['content']

                ))

                SQLOutputFormatter(
                        SummaryShareLock,
                        InfoShareLock,
                        segInfo
                )

            # If there is no DATA then write the information on the logs as NO DATA Available
            # and provide a warning message to the user and continue with the
            # rest of the steps.
            elif InfoShareLock['flag'] == 'NO DATA':

                logger.warn("Obtained no information about the share lock PID:\"{0}\", seems like"
                            " log_duration was not turned on or invalid start/end time specified".format(
                    sharelockpid
                ))

                text = "\n\n NO DATA found for the Shared lock process PID: \"{0}\" " \
                       "on segment (host/dbid/content): \"{1}/{2}/{3}\"" \
                       " Check on the Start Time and End Time" \
                       " or check on if the log_duration GUC is turned on\n\n".format(
                    sharelockpid,
                    segInfo['host'],
                    segInfo['dbid'],
                    segInfo['content']
                )

                LogFileWriter(
                    text,
                    segInfo['dbid'],
                    segInfo['host']
                )

            # Since we have finished reading the merge file lets remove them
            logger.info("Finished reading the merged file, removing the file: \"{0}\"".format(
                logfile
            ))
            os.remove(logfile)


# Function: SegmentLogReader(logfile, segInfo)
# This is the function that reads all the segment, this obtain the pid of the backup process
# and gathers the statement, number of execution and the time it took to execute
def SegmentLogReader(logfile, segInfo):

    # Local variables
    # The below parameters tells the segment log reader where you will find
    # date , pid , duration it took & the statement etc
    pDate = globalVariable.row_date
    pPid = globalVariable.row_pid
    pDuration = globalVariable.row_duration
    pQuery = globalVariable.row_query
    pDumpLocation = globalVariable.row_dumplocation
    sizeConvertor = globalVariable.sizeConvertor
    timeConvertor = globalVariable.timeConvertor

    # Variables to store the information gathered.
    SegmentProcesspid = []
    AddCopyDuration = 0
    CopyTimeCollector = {}
    SummarySegmentQuery = []
    InfoSegmentProcess = {
        'mode': 'Segment Process',
        'DBInfo': segInfo,
        'flag': 'NO DATA',
        'SegmentPid': 0,
        'totalexecution': 0,
        'totaltime': 0
    }

    # Json datafile to store the COPY data
    jsondatafile = "copy_" + segInfo['host'] + "_" + __file__ + ".data"

    # We read the master log aka Input file 2 times
    # first read to find the pid of the user that executed the share lock on the segments
    try:
        file = open(logfile, 'rb')

    # What if during this time the log was deleted by log_cleanup, since the file cannot be read
    # we will exit.
    except IOError:
        logger.error("Can't read the logfile: \"{0}\"".format(
                     logfile
        ))
        sys.exit(2)

    # Reading the file
    with file as csvfile:
        reader = csv.reader(csvfile, delimiter=',')

        logger.info("Reading the logfile to identify the share lock PID on the "
                    "log for segment (host/dbid/content): \"{0}/{1}/{2}\"".format(
            segInfo['host'],
            segInfo['dbid'],
            segInfo['content']
        ))

        # Read the file row by row
        for row in reader:

            # The only way to detect that that is the backup process is, backup
            # takes in share lock for the process so we capture that PID of the process that
            # executed the share lock and use that as the base line to hunt for
            # rest of the information
            if row[pQuery].startswith('LOCK TABLE') and row[pQuery].endswith('IN ACCESS SHARE MODE'):
                SegmentProcesspid.append(row[pPid])
                SegmentProcesspid = list(set(SegmentProcesspid))

        logger.debug("Share lock PID information obtained is: \"{0}\"".format(
                SegmentProcesspid
                     ))

        # Now if we provided the wrong dates (start time and end time),GUC log_duration was not turned ON
        # or no backup was run this can happen that we get no information of the PID,
        # lets warn and continue with the rest of the log or steps.
        if not SegmentProcesspid:
            logger.warn("Didn't find any PID that execuated share lock on the segment log, "
                        "this may result in no information logged for the share lock process")

        # This is the second read of the log file
        # Now we have obtained the pid that executed the share lock, lets gather information
        # about the statement, number of time it was executed, duration and some basic info etc...
        for segpid in SegmentProcesspid:

            logger.info("Reading the logfile to capture the information for all "
                        "things run by segment backup process: \"{0}\"".format(
                segpid
            ))

            # Move the pointer to start of the file
            file.seek(0)

            # Read again row by row
            for row in reader:

                # During this read we will try to get the size of the dump by this segment
                if row[pDumpLocation].startswith('gp_dump_agent command line'):

                    logger.info("Getting the size of the dump for the segment (host/dbid/content): \"{0}/{1}/{2}\"".format(
                        segInfo['host'],
                        segInfo['dbid'],
                        segInfo['content']
                    ))

                    # Dump file line
                    line = row[pDumpLocation]

                    # If the line has ddboost command on it.
                    if 'gpddboost' in line:

                        logger.debug("This a ddboost backup")

                        # fully qualified path
                        path = row[18].split('--to-file=')[1].split(" ")[0].rpartition('/')

                        # Filename of the dump.
                        dumplocation = path[-1]

                        # Get the file size
                        FileSize = ddboost_dump_size(dumplocation) / sizeConvertor

                        logger.debug("Backup stats (path|filename|size): \"{0}|{1}|{2}\"".format(
                                path,
                                dumplocation,
                                FileSize
                        ))

                    # If the backup is not at ddboost
                    else:

                        logger.debug("This a filesystem backup")

                        # Lets try reading the row which has the dump location information
                        dumplocation = line.split('>')[2].strip()
                        FileSize = DumpSize(dumplocation) / sizeConvertor

                        logger.debug("Backup stats (path|size): \"{0}|{1}\"".format(
                            dumplocation,
                            FileSize
                        ))

                # If the rows matches the PID and it has duration lets gather information.
                if row[pPid] == segpid and row[pDuration].split(' ')[0] == "duration:":

                    # Lets disable the flag
                    InfoSegmentProcess['flag'] = 'DATA'

                    # Get the query information
                    statement = row[pQuery]

                    # Store the PID information
                    InfoSegmentProcess['SegmentPid'] = segpid

                    # Information of the segment to be used by the copy collector
                    infoSeg = 'gpseg' + segInfo['content'] + "/dbid(" + segInfo['dbid'] + ")"

                    # Let add the Info of the segment to the copy collector is it not present
                    if infoSeg not in CopyTimeCollector:
                            CopyTimeCollector[infoSeg] = {}

                    # If the row has a COPY statement then lets get the data from it
                    if statement.startswith('COPY'):

                        # Get the table name
                        tableName = statement.split(' ')[1]

                        # Get the duration of the COPY statement
                        duration = float("%.2f" % float(float(row[pDuration].split(' ')[1]) * timeConvertor))

                        # Let keep adding the duration to know the total time
                        # it took for finish all the COPY statement
                        AddCopyDuration += duration

                        # Lets create a addon in that dict to store some basic info
                        CopyTimeCollector[infoSeg]['InfoAddonstmts'] = {}

                        # And store the information on the addons ( i.e Dump Size, Total Duration
                        CopyTimeCollector[infoSeg]['InfoAddonstmts']['SegDumpsize'] = FileSize
                        CopyTimeCollector[infoSeg]['InfoAddonstmts']['TotalstmtDuration'] = AddCopyDuration

                        # Now lets starts recording the data of duration took to backup the table
                        # on the segments
                        if tableName not in CopyTimeCollector[infoSeg]:
                            CopyTimeCollector[infoSeg][tableName] = duration

                    else:

                        # Total query executed by the PID ( incrementing each time we find one row )
                        InfoSegmentProcess['totalexecution'] += 1

                        # Total time taken by the PID ( Summing all duration each time we find one row )
                        InfoSegmentProcess['totaltime'] += float(row[pDuration].split(' ')[1])

                        # We club all the LOCK TABLE statement as ONE
                        if row[pQuery].startswith('LOCK TABLE'):
                            statement="LOCK TABLE X IN ACCESS SHARE MODE"

                        # We trim the statement into 28 character since after that some of the statement
                        # starts to add relation name which make that statement (even though the same) points as
                        # different statement. The reason for split is some SQL has statement on multi lines
                        # so when made a merge file multi lines are not placed on the file , this causes the SQL
                        # with multi lines to takes dates from the next lines, so the split ensures we only take in
                        # SQL and doesnt provide any invalid message
                        elif row[pQuery].upper().startswith('SELECT'):
                            statement = row[pQuery][0:28].split("\n")[0]

                        # We club the search_path as one
                        elif row[pQuery].upper().startswith('SET SEARCH_PATH'):
                            statement = "SET SEARCH_PATH X, pg_catalog"

                        # Rest we leave at it is
                        else:
                            statement = row[pQuery]

                        # When we receive the statement we dont have that information on the list
                        # So we make the first entry.
                        if not any(d['statement'] == statement for d in SummarySegmentQuery):
                            SummarySegmentQuery.append(
                                  {
                                     'statement' : statement,
                                     'execution' : 1,
                                     'firstdate' : row[pDate],
                                     'enddate' : row[pDate],
                                     'totalduration' : float(row[pDuration].split(' ')[1]),
                                     'maxduration' : float(row[pDuration].split(' ')[1]),
                                     'minduration' : float(row[pDuration].split(' ')[1])
                                  }
                            )

                        # Now if we receive the same statement we already have that on the list
                        else:
                            for d in SummarySegmentQuery:
                                if d['statement'] == statement:
                                    # this time we just increment
                                    d['execution'] += 1

                                    # We capture that last timestamp when this was execited
                                    d['enddate'] = row[pDate]

                                    # Sum up all the duration of this statement
                                    d['totalduration'] += float(row[pDuration].split(' ')[1])

                                    # try to find who has the highest execution time
                                    if d['maxduration'] < float(row[pDuration].split(' ')[1]):
                                        d['maxduration'] = float(row[pDuration].split(' ')[1])

                                    # try to find who has the lowest execution time
                                    if d['minduration'] > float(row[pDuration].split(' ')[1]):
                                        d['minduration'] = float(row[pDuration].split(' ')[1])

            # Reality Check, Lets check if we have data for the share lock PID
            # If we do then lets call the SQL Formatter and print the information on the log
            if InfoSegmentProcess['SegmentPid'] != 0 and InfoSegmentProcess['flag'] == 'DATA':

                # Calling the JsonWriter to store all the data from COPY to a single file
                logger.debug("Calling Json Writer to store the COPY data "
                             "for segment (host/dbid/content): \"{0}/{1}/{2}\"".format(
                    segInfo['host'],
                    segInfo['dbid'],
                    segInfo['content']

                ))
                jsonWriter(CopyTimeCollector, jsondatafile)

                # Formatting the SQL Formatter to format the SQL statements
                logger.debug("Calling SQL Formatter for segment (host/dbid/content): \"{0}/{1}/{2}\"".format(
                    segInfo['host'],
                    segInfo['dbid'],
                    segInfo['content']

                ))
                SQLOutputFormatter(
                        SummarySegmentQuery,
                        InfoSegmentProcess,
                        segInfo
                )

                # Since we have return the information lets give the information of the
                # json file to be used by the formatter.
                return jsondatafile

            # If there is no DATA then write the information on the logs as NO DATA Available
            # and provide a warning message to the user and continue with the
            # rest of the steps.
            # TODO: need to find out the error if there is no return file
            elif InfoSegmentProcess['flag'] == 'NO DATA':

                logger.warn("Obtained no information about the share lock PID:\"{0}\", seems like"
                            " log_duration was not turned on or invalid start/end time specified".format(
                    SegmentProcesspid
                ))

                text = "\n\n NO DATA found for the Shared lock process PID: \"{0}\" " \
                       "on segment (host/dbid/content): \"{1}/{2}/{3}\"" \
                       " Check on the Start Time and End Time" \
                       " or check on if the log_duration GUC is turned on\n\n".format(
                    SegmentProcesspid,
                    segInfo['host'],
                    segInfo['dbid'],
                    segInfo['content']
                )

                LogFileWriter(
                    text,
                    segInfo['dbid'],
                    segInfo['host']
                )

            # Since we have finished reading the merge file lets remove them
            logger.info("Finished reading the merged file, removing the file: \"{0}\"".format(
                logfile
            ))
            os.remove(logfile)


# Function : RunProgram()
# This function is now running on the segment servers
# This function calls all the remaining function on the script to gather than backup time information.
def RunProgram():

    # Local Variable
    tempdir = globalVariable.tempdir
    segInfo = {}
    Path = ''
    jsondatafile = None

    # Let get the Start time and end time from the OS Env.
    host = os.getenv('host1', '')
    StartTime = os.getenv('StartTime', '')
    EndTime = os.getenv('EndTime', '')
    debug = os.getenv('debug', '')

    # There is one issue the logger from now on ( i.e if its INFO / DEBUG )
    # The logger config information that was set in the Argument Parser is lost
    # as the this is a new subprocess running only this part of the information.
    # So we make some small hack to place the respective logging
    if debug == '1':
        logging.basicConfig(format=globalVariable.loggerformats, level=logging.DEBUG)
    else:
        logging.basicConfig(format=globalVariable.loggerformats, level=logging.INFO)

    logger.info("Starting the program on the host: \"{0}\"".format(
        host
    ))

    # Since we couldn't pass the timestamp with a space in between during
    # launch process, we are re-adding the space in the step below.
    StartTime = StartTime[0:10] + " " + StartTime[10:18]
    EndTime = EndTime[0:10] + " " + EndTime[10:18]

    # hostmap specific to the host is
    hostmap = "hostmap_" + host

    logger.info("Getting the segments information from the hostmap: \"{0}\"".format(
        hostmap
    ))
    # Lets get the segment information from the hostmap to work on information gathering
    if not os.path.exists(hostmap):
        logger.error("Hostmap file: \"{0}\" does not exists".format(
            hostmap
        ))
        sys.exit(2)
    else:
        SegmentInfo = StripHostmap(hostmap)

    # We should not reach here, but if we do and there is no information on the
    # hostmap_<hostname> then there is nothing to work on, lets error out.
    if not SegmentInfo:
        logger.error("There seems no information of segments on this hostmap file: \"{0}\"".format(
            hostmap
        ))
        sys.exit(2)

    # Now lets go through the segment information on the hostmap and start the reader
    logger.info("Filtering / Merging the logfile by Start time and End time for each segments")
    for segment in SegmentInfo:
        segInfo['host'] = segment[0]
        segInfo['logfile'] = segment[1]
        segInfo['dbid'] = segment[2]
        segInfo['content'] = segment[3]
        logger.debug("Received the segment information content: \"{0}\", dbid: \"{1}\"".format(
                segInfo['content'],
                segInfo['dbid']

        ))

        # We expect hostfile to be on a single location, but to be on safe side if the user provide logfile
        # at multiple location, lets take one of the logfile directory to create a inputfile.
        logfiles = segment[1].split(",")
        for logfile in logfiles:
            if os.path.isfile(logfile):
                Path = os.path.dirname(logfile)

        if not Path:
            logger.error("None of the logfile provided "
                         "for segment (host/dbid/content): \"{0}/{1}/{2}\" exists, exiting...".format(
                segInfo['host'],
                segInfo['dbid'],
                segInfo['content']
            ))
            sys.exit(2)

        # If there are multiple logfile of the same date, we will merge them to a single file
        # this helps in reducing complexity with the code and its much quicker, since in that file
        # we have only the contents from start time to the end time, so less content to read.
        for log in segInfo['logfile'].split(","):

            logger.debug("Reading / Merging / Filtering the logfile: \"{0}\" to a single file".format(
                log
            ))

            # If the file exits call the FileMerger script to Merge the contents of the logfile to a single file.
            # Once done provide us with the Input file name to do the rest of the operation.
            if os.path.exists(log):
                InputFile = InputFileMerger(Path, log, StartTime, EndTime)

            # If there is no logfile from the provided list warn the user, maybe the logfile was removed using
            # dca_log_cleanup if this is a DCA machine or some other script or its a wrong logfile name don't know..
            else:
                logger.warn("The logfile: \"{0}\" "
                             "doesn't exist on segment with host: \"{1}\", content: \"{2}\", dbid: \"{3}\"".format(
                        log,
                        segInfo['host'],
                        segInfo['content'],
                        segInfo['dbid']
                ))

        logger.debug("The merged file for the content: \"{0}\", dbid: \"{1}\" is \"{2}\"".format(
            segInfo['content'],
            segInfo['dbid'],
            InputFile

            ))

        # If the merged file is of the size zero, lets inform the user that you may get no data for that
        # segment , this can happen due to invalid start time / end time or log_duration is not turned on
        # if the log_duration not ON then segment don't log much information as most queries are run on the
        # master.
        SizeOfMergeFile = os.path.getsize(InputFile)

        if SizeOfMergeFile == 0:
            logger.warn("The merged file: \"{0}\" seems to be of the size zero, "
                        "seems like there is no contents on the file".format(
                InputFile
            ))

        # Based on the content, Lets call the respective reader
        # if the content is of the master call the master log reader
        if segInfo['content'] == "-1":

            logger.info("Calling the Master log reader to capture the information from logfile "
                        "for segment (host/dbid/content): \"{0}/{1}/{2}\"".format(
                segInfo['host'],
                segInfo['dbid'],
                segInfo['content']
            ))
            MasterLogReader(
                    InputFile,
                    segInfo
            )

        # for the rest of the content, call the segment reader
        else:
            logger.info("Calling the Segment log reader to capture the information from logfile "
                        "for segment (host/dbid/content): \"{0}/{1}/{2}\"".format(
                segInfo['host'],
                segInfo['dbid'],
                segInfo['content']
            ))
            jsondatafile = SegmentLogReader(
                    InputFile,
                    segInfo
            )

    # Okie so we are the end of the script now.
    # Let call the copy formmatter to format all the copy data from
    # all the segment of this host.
    if jsondatafile:

        logger.info("Formatting the time data obtained in backing up tables for segment host: \"{0}\"".format(
                segInfo['host']
            ))
        CopyOutputFormatter(jsondatafile, segInfo['host'])


# Function: LaunchProcess(hostmapper, StartTime, EndTime)
# The below function makes call to the segments and start the information capture.
def LaunchProcess(host, StartTime, EndTime, debug):

    logger.info("Attempting to launch the process on host: \"{0}\"".format(
        host
    ))

    # Local variables.
    py_string = os.getenv('GPHOME', '')
    tempdir = globalVariable.tempdir
    WrkDir = os.path.dirname(os.path.realpath(__file__))

    # With a space between the dates we cannot send the start time
    # and the endtime to segment, so we remove them initially and later put it back.
    StartTime = StartTime.replace(" ","")
    EndTime = EndTime.replace(" ","")

    # Let's try to guess at the python environment.  If we have GPHOME set we'll use it otherwise
    # We'll need to default to whatever is current in the shell
    if py_string:
        py_string = 'source ' + os.path.join(py_string, 'greenplum_path.sh') + '; '

    # Now let's just quick check the host as to whether the python version is >= 2.6
    logger.info("Check to ensure the version of python running is less than 2.6.0")
    try:
        subprocess.check_call(
                "ssh -T %s '%s python -c \"import sys; sys.exit(1) if sys.hexversion < 0x020600f0 else 0\"'" %
                (
                    host,
                    py_string
                ),
                shell=True
        )
    except subprocess.CalledProcessError, e:
        print >> sys.stderr, 'Python version on host " %s " is < 2.6.0.  Aborting' % (host)
        sys.exit(1)

    # Copy the script to remote site so that the script can be executed at the remote site.
    # We use /tmp since that directory is accessiable by all users of unix
    # and also the information gather by this script is not that big as well it should be in kb's
    logger.info("scp the script file: \"{0}\" to the host: \"{1}:{2}\"".format(
            __file__,
            host,
            tempdir
    ))
    try:
        subprocess.check_call(
                'scp -q %s %s:%s' %
                (
                    __file__ ,
                    host,
                    tempdir
                ),
                shell=True
        )
    except subprocess.CalledProcessError, e:
        err = 'Error when trying to copy script to %s:%s' % (host, tempdir)
        print >> sys.stderr, err
        sys.exit(1)

    # Copy the hostmap to remote site so that the script knows the segments it has to work on
    logger.info("scp the hostmap file: \"hostmap_{0}\" to the host: \"{1}:{2}\"".format(
            host,
            host,
            tempdir
    ))
    try:
        subprocess.check_call(
                'scp -q %s/hostmap_%s %s:%s' %
                (
                    tempdir,
                    host,
                    host,
                    tempdir
                ),
                shell=True
        )
    except subprocess.CalledProcessError, e:
        err = 'Error when trying to copy hostmap_%s to %s:%s' % (
            host,
            host,
            tempdir
        )
        print >> sys.stderr, err
        sys.exit(1)

    # SSH is doing something with it's terminal handling here I don't fully understand
    # If we don't force the creation of a pseudo TTY the ssh hangs
    logger.info("Launching the program to check for checking the backup time on host: \"{0}\"".format(
            host
    ))
    try:
        subprocess.check_call(
                "ssh -qtt %s \"%s cd %s; "
                "export host1=%s; "
                "export StartTime=%s; "
                "export EndTime=%s; "
                "export debug=%s; "
                "python -c 'import %s ; %s.RunProgram();' \"" %
                (
                    host,
                    py_string,
                    tempdir,
                    host,
                    '"' + StartTime + '"',
                    EndTime,
                    debug,
                    __file__.split(".")[0],
                    __file__.split(".")[0]
                ),
                shell=True
        )
    except subprocess.CalledProcessError, e:
        err = 'Error when trying to execute the script on host %s, aborting' % (host)
        print >> sys.stderr, err
        sys.exit(1)

    # Copy all the file back to the main host(i.e mostly it should be master
    # Or from the host where the script was called.
    logger.info("Copying the contents from: \"{0}:{1}\" to the main host directory: \"{2}\"".format(
            host,
            tempdir,
            WrkDir
    ))
    try:
        subprocess.check_call(
                'scp -q %s:%s/*%s.log %s' %
                (

                    host,
                    tempdir,
                    __file__,
                    WrkDir
                ),
                shell=True
        )
    except subprocess.CalledProcessError, e:
        err = 'Error when trying to copy script to %s:%s' % (host, tempdir)
        print >> sys.stderr, err
        sys.exit(1)


# Function: main()
# Go into the main program and execute the steps.
def main():

    # Local Variables.
    tempdir = globalVariable.tempdir

    # First thing first, parse the arguments passed.
    filename, StartTime, EndTime, debug = ArgumentParser(sys.argv[1:])

    # Create the temp directory on the host, if not exists
    if not os.path.exists(tempdir):
        os.makedirs(tempdir)

    # Call the parseHostfile to split the file contents into a list
    hosts = parseHostfile(
            filename
    )

    # Lets Launch the process.
    for host in hosts:
        LaunchProcess(
                host,
                StartTime,
                EndTime,
                debug
            )

    # If we reach here successfully it means we have been successful in executing
    # the script without any issues on all the host
    # so lets remove or cleanup all the work directory we have created.
    for host in hosts:
        logger.info("Removing temp work directory: \"{0}\" from host: \"{1}\"".format(
            tempdir,
            host
        ))
        RemoveTempdir(
            host,
            tempdir
    )

    # Okie so we successfully completed all the task, time to megre all the
    # files as one
    OutputFile = OutputFileMerger()

    # Success message
    logger.info("Program: \"{0}\" successfully completed".format(
        __file__
        ))

    # Logfile Location
    logger.info("Backup summary from all segments is merged onto file: \"{0}\" ".format(
        os.path.abspath(OutputFile)
        ))

# Start the main program.
if __name__ == '__main__':
    main()

// So I had this 4GB ETL trace that I wanted to open in Message Analyzer.
// Message Analyzer couldn't handle it. Crapped out. Because of a hard-coded
// limit on the size of .NET dictionary or something like that. It's 2017...
// a 4GB file isn't that big anymore. I should be able to open a 4GB file when
// I have 32GB of free RAM. 
//
//
// This code uses the Microsoft.Diagnostics.Tracing.TraceEvent which is downloadable as a NuGet package.
// This code itself is scrap. It's the TraceEvent library where the magic really is.

using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Diagnostics;
using Microsoft.Diagnostics.Tracing;

namespace ETWSplitter
{
    class ETWSplitter
    {
        static void Main(string[] args)
        {
            if (args.Count() < 3 || args.Count() > 4)
            {
                Console.WriteLine("\nUsage: ETWSplitter.exe <InputFile.etl> <OutputFile.etl> <#_of_Files> [compress]\n");
                return;
            }

            string inputFileName = args[0];
            string outFileName   = args[1];            

            if (!File.Exists(inputFileName))
            {
                Console.WriteLine("ERROR: Input file {0} was not found!", inputFileName);
                return;
            }

            if (inputFileName.Equals(outFileName, StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine("ERROR: Input file and output file have the same name!");
                return;
            }

            if (outFileName.EndsWith(".etl", StringComparison.OrdinalIgnoreCase))
            {
                outFileName = outFileName.Remove(outFileName.Length - 4);
            }            

            if (!int.TryParse(args[2], out int numberOfFiles))
            {
                Console.WriteLine("ERROR: {0} is not an integer!", args[2]);
                return;
            }

            if (numberOfFiles < 2 || numberOfFiles > 1024)
            {
                Console.WriteLine("ERROR: {0} is outside of range!", numberOfFiles);
                return;
            }

            bool compress = false;

            if (args.Count() > 3)
            {
                if (args[3].StartsWith("c", StringComparison.OrdinalIgnoreCase))
                {
                    compress = true;
                    Console.WriteLine("Compression enabled.");
                }
            }

            if (compress == false)
            {
                Console.WriteLine("Compression disabled.");
            }

            Int64 totalNumberOfEvents = 0;

            using (var etwReader = new ETWTraceEventSource(inputFileName))
            {
                etwReader.AllEvents += delegate (TraceEvent data)
                {
                    Interlocked.Increment(ref totalNumberOfEvents);
                };

                etwReader.Process();
            }

            if (totalNumberOfEvents < 2)
            {
                Console.WriteLine("ERROR: Did not detect any ETW events in the input file!");
                return;
            }

            Console.WriteLine("{0} total events found.", totalNumberOfEvents);

            Int64 numberOfEventsPerFile = totalNumberOfEvents / numberOfFiles;

            Console.WriteLine("Writing {0} files with {1} events each...", numberOfFiles, numberOfEventsPerFile);


            Int64 totalEventsWritten = 0;

            for (int fileNum = 0; fileNum < numberOfFiles; fileNum++)
            {
                Stopwatch fileTimer = new Stopwatch();

                string splitFileName = outFileName + fileNum + ".etl";

                Int64 thisFileStartIndex = numberOfEventsPerFile * fileNum;

                Int64 thisFileEventsWritten = 0;

                Int64 currentEvent = 0;

                fileTimer.Start();

                Console.Write("{0}...", splitFileName);

                using (var relogger = new ETWReloggerTraceEventSource(inputFileName, splitFileName))
                {
                    if (compress)
                    {
                        relogger.OutputUsesCompressedFormat = true;
                    }
                    else
                    {
                        relogger.OutputUsesCompressedFormat = false;
                    }

                    relogger.AllEvents += delegate (TraceEvent data)
                    {
                        if ((currentEvent >= thisFileStartIndex) && (currentEvent < (thisFileStartIndex + numberOfEventsPerFile)))
                        {
                            relogger.WriteEvent(data);
                            Interlocked.Increment(ref thisFileEventsWritten);
                            Interlocked.Increment(ref totalEventsWritten);
                        }
                        Interlocked.Increment(ref currentEvent);
                    };

                    relogger.Process();
                };

                fileTimer.Stop();

                TimeSpan timeElapsed = fileTimer.Elapsed;

                Console.WriteLine(" Done in {0:00}h:{1:00}m:{2:00}s. Wrote: {3} Filesize: {4}MB", timeElapsed.Hours, timeElapsed.Minutes, timeElapsed.Seconds, thisFileEventsWritten, (new FileInfo(splitFileName).Length) / 1024 / 1024);
            }
        }
    }
}

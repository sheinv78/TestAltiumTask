#nullable enable
using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.IO;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MoreComplexDataStructures;

namespace DataSorter
{
    internal static class Program
    {
        private static async Task<int> Main(string[] args)
        {
            var rootCommand = new RootCommand
            {
                new Argument<string>(
                    "sourcefile-name",
                    description: "File to sort from."),
                new Argument<string>(
                    "destfile-name",
                    description: "File to sort to."),
                new Option<ulong>(
                    "--chunk-size",
                    () => 0x1000U,
                    "Average chunk size in records"),
            };

            rootCommand.Description = "Data sorter application";

            var watch = System.Diagnostics.Stopwatch.StartNew();

            var prevTime = new TimeSpan();

            // Note that the parameters of the handler method are matched according to the names of the options
            rootCommand.Handler = CommandHandler.Create(
                async (string sourcefileName, string destFileName, ulong chunkSize,
                    IConsole console, CancellationToken ctoken) =>
                {
                    ctoken.Register(() =>
                    {
                        console.Error.WriteLine();
                        console.Error.WriteLine("Operation was cancelled");
                    });

                    var progress = new Progress<ulong>(
                        _ =>
                        {
                            if ((watch.Elapsed - prevTime).TotalSeconds < 1) return;

                            prevTime = watch.Elapsed;

                            UpdateStatus(console, prevTime);
                        });

                    await SortFile(sourcefileName, destFileName, chunkSize, console, ctoken, progress);
                });

            // Parse the incoming args and invoke the handler
            var result = await rootCommand.InvokeAsync(args);
            watch.Stop();

            Console.WriteLine();
            Console.Write($"Total execution time: {watch.Elapsed:c}");

            return result;
        }

        private static Task SortFile(string sourcefileName, string destFileName, ulong chunkSize,
            IStandardOut console,
            CancellationToken ctoken,
            IProgress<ulong> progress)
        {
            return Task.Run(() =>
            {
                console.Out.WriteLine("Splitting...");

                // Step1: Split input file into chunks of size ChunkSize.
                var fileNames = Split(sourcefileName, chunkSize);

                var fileList = fileNames.ToList();
                foreach (var fileName in fileList)
                {
                    console.Out.WriteLine(fileName);
                }

                // Step2: Merging chunks
                KWayMerge(fileList, destFileName);

                foreach (var fileName in fileList)
                {
                    File.Delete(fileName);
                    console.Out.WriteLine($"{fileName} deleted.");
                }

                console.Out.WriteLine($"Dest file: {destFileName}");
            }, ctoken);
        }

        private static IEnumerable<string> Split(string sourcefileName, ulong chunkSize)
        {
            static string WriteSortedData(List<FileData> list)
            {
                list.Sort();

                var fileName = Path.GetTempFileName();

                using var fileWriter = File.CreateText(fileName);

                foreach (var data in list)
                {
                    fileWriter.WriteLine(data.ToString());
                }

                fileWriter.Close();
                list.Clear();
                return fileName;
            }

            var tempFileNames = new List<string>();

            using var srcReader = File.OpenText(sourcefileName);

            var chunk = new List<FileData>((int) chunkSize);

            while (!srcReader.EndOfStream)
            {
                var line = srcReader.ReadLine();

                if (line == null)
                    break;

                var fileData = FileData.FromString(line);

                if (chunk.Count < chunk.Capacity)
                {
                    chunk.Add(fileData);
                }
                else
                {
                    var tempfileName = WriteSortedData(chunk);
                    tempFileNames.Add(tempfileName);
                }
            }

            if (chunk.Count == 0) return tempFileNames;

            var fileName = WriteSortedData(chunk);
            tempFileNames.Add(fileName);

            return tempFileNames;
        }

        private static void KWayMerge(IEnumerable<string> fileNames, string destFileName)
        {
            using var destWriter = File.CreateText(destFileName);

            var streamReaders = new List<(StreamReader,string)>();

            try
            {
                streamReaders.AddRange(fileNames.Select(x=>(File.OpenText(x),x)));

                var queue = new MinHeap<FileDataReaderItem>();

                foreach (var (reader, fileName) in streamReaders)
                {
                    var line = reader.ReadLine();
                    if (line == null)
                        continue;

                    var fileData = FileData.FromString(line);

                    queue.Insert(new FileDataReaderItem(fileData, reader, fileName));
                }

                while (true)
                {
                    if (queue.Count == 0) break;

                    var minItem = queue.ExtractMin();

                    if (minItem == null) break;

                    destWriter.WriteLine(minItem.FileData.ToString());

                    var line = minItem.StreamReader.ReadLine();

                    if (line == null)
                    {
                        minItem.StreamReader.Close();
                        File.Delete(minItem.FileName);
                        continue;
                    }

                    var newFileData = FileData.FromString(line!);

                    queue.Insert(new FileDataReaderItem(newFileData, minItem.StreamReader, minItem.FileName));
                }
            }
            finally
            {
                foreach (var (reader, fileName) in streamReaders)
                {
                    reader.Close();

                    if (File.Exists(fileName))
                        File.Delete(fileName);
                }
            }
        }

        /// <summary>
        ///     Update status.
        /// </summary>
        /// <param name="console">Console</param>
        /// <param name="timeSpan">Elapsed time.</param>
        private static void UpdateStatus(IStandardOut console, TimeSpan timeSpan)
        {
            console.Out.Write($"\rPassed: {timeSpan:c}");
        }
    }
}

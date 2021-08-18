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
                    () => 0x100000U,
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

                    var progress = new Progress<string>(
                        value =>
                        {
                            if ((watch.Elapsed - prevTime).TotalSeconds < 1) return;

                            prevTime = watch.Elapsed;

                            UpdateStatus(console, prevTime, value);
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

        /// <summary>
        ///     Sort file.
        /// </summary>
        /// <param name="sourcefileName">Source file name.</param>
        /// <param name="destFileName">Destination file name.</param>
        /// <param name="chunkSize">The size of single chunk.</param>
        /// <param name="console">Console.</param>
        /// <param name="ctoken">Cancellation tocken.</param>
        /// <param name="progress">Progress to report to the user.</param>
        /// <returns>Task.</returns>
        private static Task SortFile(string sourcefileName, string destFileName, ulong chunkSize,
            IStandardOut console,
            CancellationToken ctoken,
            IProgress<string> progress)
        {
            return Task.Run(() =>
            {
                console.Out.WriteLine("Splitting...");

                var sourceFileInfo = new FileInfo(sourcefileName);

                // ReSharper disable once PossibleLossOfFraction
                var countOfFiles = Math.Ceiling((decimal)(sourceFileInfo.Length / (long)chunkSize));

                console.Out.WriteLine($"Estimated split into {countOfFiles} files");

                // Step1: Split input file into chunks of size ChunkSize.
                var fileNames = Split(sourcefileName, chunkSize, progress);

                var fileList = fileNames.ToList();
                foreach (var fileName in fileList)
                {
                    console.Out.WriteLine(fileName);
                }

                // Step2: Merging chunks
                KWayMerge(fileList, destFileName, progress);

                foreach (var fileName in fileList)
                {
                    File.Delete(fileName);
                    console.Out.WriteLine($"{fileName} deleted.");
                }

                console.Out.WriteLine($"Dest file: {destFileName}");
            }, ctoken);
        }

        /// <summary>
        ///     Split source file into a set of sorted chunks.
        /// </summary>
        /// <param name="sourcefileName">The name of source file.</param>
        /// <param name="chunkSize">The size of single file.</param>
        /// <param name="progress"></param>
        /// <returns>The list of file names containing presorted chunks.</returns>
        private static IEnumerable<string> Split(string sourcefileName, ulong chunkSize, IProgress<string> progress)
        {
            static string WriteSortedData(List<FileData> list, IProgress<string> progress)
            {
                list.Sort();

                var fileName = Path.GetTempFileName();

                progress.Report($"Start unloading the chunk into {fileName}");

                using var fileWriter = File.CreateText(fileName);

                foreach (var data in list)
                {
                    fileWriter.WriteLine(data.ToString());
                }

                progress.Report($"Unloading the chunk into {fileName} completed");

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
                    var tempfileName = WriteSortedData(chunk, progress);
                    tempFileNames.Add(tempfileName);
                }
            }

            if (chunk.Count == 0) return tempFileNames;

            var fileName = WriteSortedData(chunk, progress);
            tempFileNames.Add(fileName);

            return tempFileNames;
        }

        /// <summary>
        ///     Perform K-way merge.
        /// </summary>
        /// <param name="fileNames">Source file names.</param>
        /// <param name="destFileName">Destination file name.</param>
        /// <param name="progress"></param>
        private static void KWayMerge(IEnumerable<string> fileNames, string destFileName, IProgress<string> progress)
        {
            using var destWriter = File.CreateText(destFileName);

            var streamReaders = new List<(StreamReader,string)>();

            try
            {
                progress.Report($"Initial fillup of minheap.");

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

                progress.Report($"Start k-way merge.");

                while (true)
                {
                    if (queue.Count == 0) break;

                    var minItem = queue.ExtractMin();

                    if (minItem == null)
                    {
                        progress.Report("Sucessfully completed.");
                        break;
                    }

                    destWriter.WriteLine(minItem.FileData.ToString());

                    var line = minItem.StreamReader.ReadLine();

                    if (line == null)
                    {
                        progress.Report($"The chunk: {minItem.FileName} depleted.");
                        minItem.StreamReader.Close();
                        File.Delete(minItem.FileName);
                        continue;
                    }

                    var newFileData = FileData.FromString(line);

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
        /// <param name="value"></param>
        private static void UpdateStatus(IStandardOut console, TimeSpan timeSpan, string value)
        {
            console.Out.Write($"\r{new string(' ', 80)}");
            var str = $"Passed: {timeSpan:c} {value}";
            console.Out.Write($"\r{str, 80}");
        }
    }
}

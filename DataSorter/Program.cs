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

namespace DataSorter
{
    internal static class Program
    {
        private record DataItem(FileData? Item, FileDataReader StreamReader, string FileName);

        private static async Task Main(string[] args)
        {
            var rootCommand = new RootCommand
            {
                new Argument<string>(
                    "sourcefile-name",
                    description: "File to sort from."),
                new Argument<string>(
                    "destfile-name",
                    description: "File to sort to."),
                new Option<int>(
                    "--chunk-size",
                    () => 1_000_000,
                    "Average chunk size in records"),
            };

            rootCommand.Description = "Data sorter application";

            var watch = System.Diagnostics.Stopwatch.StartNew();

            // Note that the parameters of the handler method are matched according to the names of the options
            rootCommand.Handler = CommandHandler.Create(
                async (string sourcefileName, string destFileName, int chunkSize,
                    IConsole console, CancellationToken cToken) =>
                {
                    cToken.Register(() =>
                    {
                        console.Error.WriteLine();
                        console.Error.WriteLine("Operation was cancelled");
                    });

                    var progress = new Progress<string>(
                        value => { UpdateStatus(console, value); });

                    await SortFile(sourcefileName, destFileName, chunkSize, console, cToken, progress);
                });

            // Parse the incoming args and invoke the handler
            var cmdTask = rootCommand.InvokeAsync(args);

            cmdTask.GetAwaiter().OnCompleted(() =>
            {
                watch.Stop();

                Console.WriteLine();
                Console.Write($"Total execution time: {watch.Elapsed:c}");
            });

            await cmdTask;
        }

        /// <summary>
        ///     Sort file.
        /// </summary>
        /// <param name="sourceFileName">Source file name.</param>
        /// <param name="destFileName">Destination file name.</param>
        /// <param name="chunkSize">The size of single chunk.</param>
        /// <param name="console">Console.</param>
        /// <param name="cToken">Cancellation token.</param>
        /// <param name="progress">Progress to report to the user.</param>
        /// <returns>Task.</returns>
        private static async Task SortFile(string sourceFileName, string destFileName, int chunkSize,
            IStandardOut console,
            CancellationToken cToken,
            IProgress<string> progress)
        {
            try
            {
                using var srcReader = DataFile.OpenFileDataReader(sourceFileName);

                progress.Report("Splitting into chunks...");

                // Step1: Split input file into chunks of size ChunkSize.
                var files = await SplitFile(srcReader, chunkSize, @"C:\Temp", cToken, progress);

                await using var dstWriter = DataFile.OpenFileDataWriter(destFileName);

                // Step2: Merging chunks
                await KWayMerge(files, dstWriter, cToken, progress);

                progress.Report($"Dest file: {destFileName}");
            }
            catch (Exception e)
            {
                progress.Report(e.Message);
            }
        }

        // private static void Cleanup(IEnumerable<string> fileNames)
        // {
        //     Parallel.ForEach(fileNames, File.Delete);
        // }

        /// <summary>
        ///     Split source file into a set of sorted chunks.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="chunkSize"></param>
        /// <param name="cToken"></param>
        /// <param name="progress"></param>
        /// <param name="path"></param>
        /// <returns>The list of file names containing presorted chunks.</returns>
        private static async Task<List<string>> SplitFile(FileDataReader reader, int chunkSize, string path,
            CancellationToken cToken, IProgress<string> progress)
        {
            var result = new List<string>();

            progress.Report("Splitting started");

            var list = new List<FileData>(chunkSize);

            while (!reader.EndOfStream)
            {
                progress.Report("Preparing next chunk.");
                while (list.Count < list.Capacity)
                {
                    cToken.ThrowIfCancellationRequested();

                    var item = await reader.ReadFileDataItemAsync();

                    if (item is not null)
                    {
                        list.Add(item);
                    }
                    else
                        break;
                }

                var sortedData = list.OrderBy(x => x.StringPart);

                var fileName = Path.Combine(path, Path.GetRandomFileName());
                result.Add(fileName);

                await using var fileWriter =
                    DataFile.CreateFileDataWriter(fileName);

                await fileWriter.WriteFileDataAsync(sortedData);

                progress.Report($"{fileName} finished");

                list.Clear();
            }

            return result;
        }

        /// <summary>
        ///     Perform K-way merge.
        /// </summary>
        /// <param name="files"></param>
        /// <param name="writer"></param>
        /// <param name="cancellationToken"></param>
        /// <param name="progress"></param>
        /// <param name="fileNames">Source file names.</param>
        /// <param name="destFileName">Destination file name.</param>
        private static async Task KWayMerge(IEnumerable<string> files, FileDataWriter writer,
            CancellationToken cancellationToken,
            IProgress<string> progress)
        {
            List<FileDataReader>? dataReaders = null;

            try
            {
                dataReaders = files.Select(DataFile.OpenFileDataReader).ToList();

                progress.Report($"Initial fill up of minheap.");

                var queue = new PriorityQueue<DataItem, FileData>();

                foreach (var dataReader in dataReaders)
                {
                    var fileData = await dataReader.ReadFileDataItemAsync()!;

                    if (fileData != null)
                        queue.Enqueue(new DataItem(fileData, dataReader, dataReader.FileName), fileData);
                }

                progress.Report($"Start k-way merge.");

                while (queue.Count > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var (item, streamReader, fileName) = queue.Dequeue();

                    await writer.WriteFileDataAsync(item);

                    var newItem = await streamReader.ReadFileDataItemAsync();

                    if (newItem is null)
                    {
                        progress.Report($"The chunk: {fileName} depleted.");
                        streamReader.Close();
                        dataReaders.Remove(streamReader);
                        File.Delete(fileName);
                    }
                    else
                    {
                        queue.Enqueue(new DataItem(newItem, streamReader, fileName), newItem);
                    }
                }

                progress.Report("Successfully completed.");
            }
            finally
            {
                if (dataReaders != null)
                    foreach (var reader in dataReaders)
                    {
                        reader.Dispose();
                    }
            }
        }

        /// <summary>
        ///     Update status.
        /// </summary>
        /// <param name="console">Console</param>
        /// <param name="value"></param>
        private static void UpdateStatus(IStandardOut console, string value)
        {
            console.Out.WriteLine(value);
        }
    }
}

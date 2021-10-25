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
        private record DataItem(FileData? Item, FileDataReader StreamReader);

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

                    await SortFile(sourcefileName, destFileName, chunkSize, cToken, progress);
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
        /// <param name="cToken">Cancellation token.</param>
        /// <param name="progress">Progress to report to the user.</param>
        /// <returns>Task.</returns>
        private static async Task SortFile(string sourceFileName, string destFileName, int chunkSize,
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

            try
            {
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
            }
            catch (Exception)
            {
                foreach (var fileName in result.Where(File.Exists))
                {
                    File.Delete(fileName);
                    progress.Report($"{reader.FileName} was deleted");
                }

                result.Clear();

                throw;
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
                    cancellationToken.ThrowIfCancellationRequested();

                    var fileData = await dataReader.ReadFileDataItemAsync();

                    if (fileData != null)
                        queue.Enqueue(new DataItem(fileData, dataReader), fileData);
                }

                progress.Report($"Start k-way merge.");

                while (queue.Count > 0)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var (item, streamReader) = queue.Dequeue();

                    await writer.WriteFileDataAsync(item);

                    var newItem = await streamReader.ReadFileDataItemAsync();

                    if (newItem is null)
                    {
                        progress.Report($"The chunk: {streamReader.FileName} depleted.");
                        streamReader.Close();
                        dataReaders.Remove(streamReader);
                        File.Delete(streamReader.FileName);
                    }
                    else
                    {
                        queue.Enqueue(new DataItem(newItem, streamReader), newItem);
                    }
                }

                progress.Report("Successfully completed.");
            }
            finally
            {
                if (dataReaders != null)
                    foreach (var reader in dataReaders)
                    {
                        reader.Close();
                        if (!File.Exists(reader.FileName)) continue;
                        File.Delete(reader.FileName);
                        progress.Report($"{reader.FileName} was deleted");
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

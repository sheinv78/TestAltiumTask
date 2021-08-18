using System;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.CommandLine.IO;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Bogus;

namespace DataGen
{
    // ReSharper disable once UnusedType.Global
    internal static class Program
    {
        // ReSharper disable once UnusedMember.Local
        private static async Task<int> Main(string [] args)
        {
            // Create a root command with some options
            var rootCommand = new RootCommand
            {
                new Argument<string>(
                    "fileName",
                    description: "File to write data to."),
                new Option<ulong>(
                    "--count",
                    ()=>100000000UL,
                    "Number of the lines to write."),
                new Option<uint>(
                    "--lowerBound",
                    ()=>1000,
                    "Lower bound of the range of the numeric part."),
                new Option<uint>(
                    "--upperBound",
                    ()=>1005,
                    "Upper bound of the range of the numeric part."),
                new Option<int>(
                    "--minWords",
                    ()=>1,
                    "Minimum number of words in the string part."),
                new Option<int>(
                    "--maxWords",
                    ()=>2,
                    "Maximum number of words in the string part.")
            };

            rootCommand.Description = "Data generator application";

            var completionPercent = -1;

            // Note that the parameters of the handler method are matched according to the names of the options
            rootCommand.Handler = CommandHandler.Create(async (string fileName, ulong count, uint lowerBound, uint upperBound, int minWords, int maxWords, IConsole console, CancellationToken ctoken) =>
            {
                ctoken.Register(() =>
                {
                    console.Error.WriteLine();
                    console.Error.WriteLine("Operation was cancelled");
                });

                var progress = new Progress<ulong>(
                    value =>
                    {
                        var newCompletionPercent = Convert.ToInt32(Math.Round(value*1.0f/count * 100));

                        if (newCompletionPercent == completionPercent) return;

                        UpdateStatus(console, newCompletionPercent);
                        completionPercent = newCompletionPercent;
                    });

                await GenerateFile(fileName, count, lowerBound, upperBound, minWords, maxWords, ctoken, progress);
            });

            var watch = System.Diagnostics.Stopwatch.StartNew();
            // Parse the incoming args and invoke the handler
            var result = await rootCommand.InvokeAsync(args);
            watch.Stop();

            Console.WriteLine();
            Console.Write($"Total execution time: {watch.Elapsed:c}");

            return result;
        }

        /// <summary>
        ///     Update status.
        /// </summary>
        /// <param name="console">Console</param>
        /// <param name="newCompletionPercent">New value of completion percent.</param>
        private static void UpdateStatus(IStandardOut console, int newCompletionPercent)
        {
            console.Out.Write(newCompletionPercent == 100 ? "\rDone" : $"\r{newCompletionPercent}%");
        }

        /// <summary>
        ///     Data file generator.
        /// </summary>
        /// <param name="fileName">File name to write data into.</param>
        /// <param name="count">Count of lines in resulting file.</param>
        /// <param name="lowerBound">Lower bound of index part.</param>
        /// <param name="upperBound">Upper bound of index part.</param>
        /// <param name="minWords">Minimum words in string part.</param>
        /// <param name="maxWords">Maximum words in string part.</param>
        /// <param name="token">Cancellation token.</param>
        /// <param name="progress">Progresser to notify to.</param>
        /// <returns>Task.</returns>
        [SuppressMessage("ReSharper.DPA", "DPA0002: Excessive memory allocations in SOH")]
        private static async Task GenerateFile(string fileName, ulong count, uint lowerBound, uint upperBound, int minWords, int maxWords, CancellationToken token, IProgress<ulong> progress)
        {
            await using var file = File.CreateText(fileName);

            Randomizer.Seed = new Random((int) DateTime.Now.Ticks);

            var sb = new StringBuilder(Environment.SystemPageSize);

            var faker = new Faker("en_US");

            for (var i = 0UL; i < count; i++)
            {
                if (token.IsCancellationRequested)
                {
                    return;
                }

                sb.AppendJoin(". ", faker.Random.UInt(lowerBound, upperBound),
                    faker.Lorem.Sentence(minWords, maxWords - minWords));

                await file.WriteLineAsync(sb.ToString());
                progress.Report(i);

                sb.Clear();
            }
        }
    }
}

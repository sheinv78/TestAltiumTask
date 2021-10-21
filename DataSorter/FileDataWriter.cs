using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DataSorter
{
    public class FileDataWriter: StreamWriter
    {

        #region Base constructors

        public FileDataWriter(Stream stream) : base(stream)
        {
        }

        public FileDataWriter(Stream stream, Encoding encoding) : base(stream, encoding)
        {
        }

        public FileDataWriter(Stream stream, Encoding encoding, int bufferSize) : base(stream, encoding, bufferSize)
        {
        }

        public FileDataWriter(Stream stream, Encoding? encoding = null, int bufferSize = -1, bool leaveOpen = false) : base(stream, encoding, bufferSize, leaveOpen)
        {
        }

        public FileDataWriter(string path) : base(path)
        {
        }

        public FileDataWriter(string path, bool append) : base(path, append)
        {
        }

        public FileDataWriter(string path, bool append, Encoding encoding) : base(path, append, encoding)
        {
        }

        public FileDataWriter(string path, bool append, Encoding encoding, int bufferSize) : base(path, append, encoding, bufferSize)
        {
        }

        #endregion

        public async Task WriteFileDataAsync(IAsyncEnumerable<FileData?> fileData)
        {
            await foreach (var fileDataItem in fileData)
            {
                await WriteFileDataAsync(fileDataItem);
            }
        }

        public async Task WriteFileDataAsync(IEnumerable<FileData?> fileData)
        {
            foreach (var fileDataItem in fileData)
            {
                await WriteFileDataAsync(fileDataItem);
            }
        }

        public async Task WriteFileDataAsync(FileData? fileData)
        {
            _ = fileData ?? throw new ArgumentNullException(nameof(fileData));

            await WriteLineAsync(fileData.ToString());
        }
    }
}

using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace DataSorter
{
    public class FileDataReader: StreamReader
    {
        #region Base constructors

        public FileDataReader(Stream stream) : base(stream)
        {
        }

        public FileDataReader(Stream stream, bool detectEncodingFromByteOrderMarks) : base(stream, detectEncodingFromByteOrderMarks)
        {
        }

        public FileDataReader(Stream stream, Encoding encoding) : base(stream, encoding)
        {
        }

        public FileDataReader(Stream stream, Encoding encoding, bool detectEncodingFromByteOrderMarks) : base(stream, encoding, detectEncodingFromByteOrderMarks)
        {
        }

        public FileDataReader(Stream stream, Encoding encoding, bool detectEncodingFromByteOrderMarks, int bufferSize) : base(stream, encoding, detectEncodingFromByteOrderMarks, bufferSize)
        {
        }

        public FileDataReader(Stream stream, Encoding? encoding = null, bool detectEncodingFromByteOrderMarks = true, int bufferSize = -1, bool leaveOpen = false) : base(stream, encoding, detectEncodingFromByteOrderMarks, bufferSize, leaveOpen)
        {
        }

        public FileDataReader(string path) : base(path)
        {
        }

        public FileDataReader(string path, bool detectEncodingFromByteOrderMarks) : base(path, detectEncodingFromByteOrderMarks)
        {
        }

        public FileDataReader(string path, Encoding encoding) : base(path, encoding)
        {
        }

        public FileDataReader(string path, Encoding encoding, bool detectEncodingFromByteOrderMarks) : base(path, encoding, detectEncodingFromByteOrderMarks)
        {
        }

        public FileDataReader(string path, Encoding encoding, bool detectEncodingFromByteOrderMarks, int bufferSize) : base(path, encoding, detectEncodingFromByteOrderMarks, bufferSize)
        {
        }

        public string FileName
        {
            get
            {
                if (BaseStream is FileStream fs)
                {
                    return fs.Name;
                }

                return string.Empty;
            }
        }

        #endregion

        public async IAsyncEnumerable<FileData?> ReadFileDataEnumAsync()
        {
            while (!EndOfStream)
            {
                var line = (await ReadLineAsync())!;
                yield return FileData.FromString(line);
            }
        }

        public async Task<FileData?> ReadFileDataItemAsync()
        {
            if (EndOfStream)
            {
                return await Task.FromResult<FileData?>(null);
            }

            var line = (await ReadLineAsync())!;

            return FileData.FromString(line);
        }
    }
}

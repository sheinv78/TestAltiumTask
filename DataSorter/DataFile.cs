using System;

namespace DataSorter
{
    public static class DataFile
    {
        public static FileDataReader OpenFileDataReader(string path)
        {
            _ = path ?? throw new ArgumentNullException(nameof(path));

            return new FileDataReader(path);
        }

        public static FileDataWriter OpenFileDataWriter(string path)
        {
            _ = path ?? throw new ArgumentNullException(nameof(path));

            return new FileDataWriter(path);
        }

        public static FileDataWriter CreateFileDataWriter(string path)
        {
            _ = path ?? throw new ArgumentNullException(nameof(path));

            return new FileDataWriter(path, false);
        }
    }
}

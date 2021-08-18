using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;

namespace DataSorter
{
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
    internal class FileDataReaderItem : IComparable<FileDataReaderItem>
    {
        public FileDataReaderItem(FileData fileData, StreamReader streamReader, string fileName)
        {
            FileData = fileData;
            StreamReader = streamReader;
            FileName = fileName;
        }

        public FileData FileData { get; set; }
        public StreamReader StreamReader { get; set; }

        public string FileName { get; set; }

        public int CompareTo(FileDataReaderItem? other)
        {
            return FileData.CompareTo(other?.FileData);
        }
    }
}
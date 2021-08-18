using System;
using System.Text;

namespace DataSorter
{
    internal class FileData : IComparable<FileData>
    {
        private FileData()
        {
            NumericPart = 0;
            StringPart = string.Empty;
        }

        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once PropertyCanBeMadeInitOnly.Global
        public uint NumericPart { get; set; }

        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once PropertyCanBeMadeInitOnly.Global
        public string StringPart { get; set; }

        public static FileData FromString(string str)
        {
            var parts = str.Split(". ");

            if (parts.Length != 2)
            {
                throw new InvalidOperationException($"The string {str} couldn't be parsed into FileData instance.");
            }

            return new FileData()
            {
                NumericPart = uint.Parse(parts[0]),
                StringPart = parts[1]
            };
        }

        public int CompareTo(FileData? other)
        {
            if (other == null)
                return 1;

            var stringCmpResult = string.Compare(StringPart, other.StringPart, StringComparison.Ordinal);

            return stringCmpResult != 0 ? stringCmpResult : NumericPart.CompareTo(other.NumericPart);
        }

        public override string ToString()
        {
            return new StringBuilder().AppendJoin(". ", NumericPart, StringPart).ToString();
        }
    }
}
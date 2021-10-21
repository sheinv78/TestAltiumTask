#nullable enable
using System;
using System.Text;

namespace DataSorter
{
    public record FileData : IComparable<FileData>
    {
        private const string Delimiter = ". ";

        private FileData()
        {
            NumericPart = "0";
            StringPart = string.Empty;
        }

        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once PropertyCanBeMadeInitOnly.Global
        public string NumericPart { get; init; }

        // ReSharper disable once MemberCanBePrivate.Global
        // ReSharper disable once PropertyCanBeMadeInitOnly.Global
        public string StringPart { get; init; }

        public static FileData? FromString(string str)
        {
            var span = str.AsSpan();

            var delimiterIdx = span.IndexOf(Delimiter);

            if (delimiterIdx == -1)
                throw new IndexOutOfRangeException("The delimiter was not found");

            var numericPart = span[..(delimiterIdx)].Trim();
            var stringPart = span[(delimiterIdx + 1)..].Trim();

            return new FileData()
            {
                NumericPart = numericPart.ToString(),
                StringPart = stringPart.ToString()
            };
        }

        public int CompareTo(FileData? other)
        {
            if (other == null)
                return 1;

            var stringCmpResult = string.Compare(StringPart, other.StringPart, StringComparison.Ordinal);

            return stringCmpResult != 0
                ? stringCmpResult
                : string.Compare(NumericPart, other.NumericPart, StringComparison.Ordinal);
        }

        public override string ToString()
        {
            return new StringBuilder().AppendJoin(". ", NumericPart, StringPart).ToString();
        }
    }
}

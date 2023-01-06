using ICSharpCode.SharpZipLib.BZip2;
using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Lzw;
using System;
using System.Diagnostics.Metrics;
using System.Reflection.PortableExecutable;

namespace DataUtilities.CSVMerge // Note: actual namespace depends on the project name.
{
    internal class Program
    {
        private enum Enum_FileType
        {
            txt = 0,
            gzip = 1,
            bzip = 2,
            lzw = 3,
        }
        static void Main(string[] args)
        {
            bool argsValid = (args.Length >= 5);
            int firstFileStartRow = 0, otherFilesStartRow = 0;
            string inputFolder = String.Empty;
            string outputFile = String.Empty;
            Enum_FileType fileType = Enum_FileType.txt;
            int OutputFileCount = 1;
            string FilenameRegex;

            if (argsValid)
            {
                if (!int.TryParse(args[0], out firstFileStartRow))
                {
                    argsValid = false;
                }
                if (!int.TryParse(args[1], out otherFilesStartRow))
                {
                    argsValid = false;
                }
                if (firstFileStartRow < 0)
                {
                    argsValid = false;
                }
                if (otherFilesStartRow < 0)
                {
                    argsValid = false;
                }
                object tempValue;
                if (Enum.TryParse(typeof(Enum_FileType), args[2], out tempValue))
                {
                    fileType = (Enum_FileType)tempValue;
                }
                else
                {
                    argsValid = false;
                }
                inputFolder = args[3].Trim();
                outputFile = args[4].Trim();
                if (string.IsNullOrEmpty(inputFolder))
                {
                    argsValid = false;
                }
                if (string.IsNullOrEmpty(outputFile))
                {
                    argsValid = false;
                }
                if (args.Length >= 6)
                {
                    if (!int.TryParse(args[5], out OutputFileCount))
                    {
                        OutputFileCount = 1;
                    }
                }
                if (args.Length >= 7)
                {
                    FilenameRegex = args[6];
                    FilenameRegex = FilenameRegex.Trim();
                    if (!int.TryParse(args[5], out OutputFileCount))
                    {
                        OutputFileCount = 1;
                    }
                }
            }
            if (!argsValid)
            {
                Console.WriteLine(@"Usage: CSVMerge.exe <First File Start Row> <Other Files Start Row> <File Format> <Source Path Folder> <Destination File> <Output File Count>");
                Console.WriteLine(@"");
                Console.WriteLine(@"First File Start Row      Number of rows to skip from start of first file");
                Console.WriteLine(@"Other Files Start Row     Number of rows to skip from start of subsequent files");
                Console.WriteLine(@"File Format               txt | gzip | bzip | lzw");
                Console.WriteLine(@"Source Path Folder        Folder containing files to be merged");
                Console.WriteLine(@"Destination File          Output merged file. If file already exists it will be overwritten");
                Console.WriteLine(@"Output File Count         Default is 1. Any other number will create _1 _2 etc.. outputs");
                Console.WriteLine(@"");
                Console.WriteLine("Example: CSVMerge.exe 1 1 txt \"C:\\Temp\\My Exported Files\" C:\\Temp\\SingleFile.csv");
                Console.WriteLine(@"");
                Console.WriteLine("Example: CSVMerge.exe 0 1 gzip \"C:\\Temp\\My Exported Files\" \"C:\\Temp\\Merged Data.csv\"");
                Console.WriteLine(@"");
                Console.WriteLine("Example: CSVMerge.exe 12 1 bzip \"C:\\Temp\\My Exported Files\" C:\\Temp\\SingleFile.csv");
                Console.WriteLine(@"");
                Console.WriteLine("Example: CSVMerge.exe 0 1 txt \"C:\\Temp\\My Exported Files\" C:\\Temp\\ThreeFiles.csv 3");
                Console.WriteLine(@"");
                Console.WriteLine(@"Note: default directory file sort order will be used for ordering the files");
                Console.WriteLine(@"Example: ABC000.txt, ABC001.txt, ABC003.txt");
                Console.WriteLine(@"In this example ABC000.txt will be the first file");
                return;
            }
            if (!System.IO.Directory.Exists(inputFolder))
            {
                Console.WriteLine(@"Error: Source Path Folder does not exist or access denied");
                return;
            }
            string outputFolder;
            outputFolder = System.IO.Path.GetDirectoryName(outputFile);
            if (!System.IO.Directory.Exists(outputFolder))
            {
                try
                {
                    System.IO.Directory.CreateDirectory(outputFolder);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(@"Error: Destination file path is either invalid or access denied");
                    Console.WriteLine(ex.Message);
                    return;
                }
            }
            List<System.IO.TextWriter> TRList = new List<System.IO.TextWriter>();
            System.IO.TextWriter TR;
            try
            {
                if (OutputFileCount == 1)
                {
                    if (System.IO.File.Exists(outputFile))
                    {
                        System.IO.File.Delete(outputFile);
                    }
                    TR = System.IO.File.CreateText(outputFile);
                    TRList.Add(TR);
                }
                else
                {
                    string? folder;
                    string filename;
                    string extension;
                    string appendText;
                    string chunkFileName;
                    int outputCountLength;
                    folder = System.IO.Path.GetDirectoryName(outputFile);
                    if (string.IsNullOrEmpty(folder)) { folder = string.Empty; }
                    else { folder = string.Concat(folder.TrimEnd(@"\/".ToCharArray()), @"\"); }
                    filename = System.IO.Path.GetFileNameWithoutExtension(outputFile);
                    extension = System.IO.Path.GetExtension(outputFile);
                    outputCountLength = OutputFileCount.ToString().Length;
                    for (int i = 1; i <= OutputFileCount; i++)
                    {
                        appendText = i.ToString();
                        if (outputCountLength > appendText.Length)
                        {
                            appendText = appendText.PadLeft(outputCountLength, '0');
                        }
                        chunkFileName = string.Concat(folder, filename, @"_", appendText, extension);
                        if (System.IO.File.Exists(chunkFileName))
                        {
                            System.IO.File.Delete(chunkFileName);
                        }
                        TR = System.IO.File.CreateText(chunkFileName);
                        TRList.Add(TR);
                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(@"Error: Destination file path is either invalid or access denied");
                Console.WriteLine(ex.Message);
                return;
            }
            try
            {
                DirectoryInfo dirInput;
                dirInput = new DirectoryInfo(inputFolder);
                FileInfo[] inputFileList;
                inputFileList = dirInput.GetFiles();
                int numFiles = inputFileList.Length;
                if (numFiles == 0)
                {
                    Console.WriteLine(@"Error: Source Path Folder does not contain any files");
                    return;
                }
                bool isFirst = true;
                bool isFirstFile = true;
                int skipRows;
                int progressFileCounter = 0;
                foreach (FileInfo inputFile in inputFileList)
                {
                    progressFileCounter += 1;
                    Console.WriteLine(String.Format(@"Progress: {0} of {1} :- {2}", progressFileCounter, numFiles, inputFile.Name));
                    isFirstFile = isFirst;
                    if (isFirst)
                    {
                        isFirst = false;
                        skipRows = firstFileStartRow;
                    }
                    else
                    {
                        skipRows = otherFilesStartRow;
                    }
                    switch (fileType)
                    {
                        case Enum_FileType.txt:
                            using (FileStream fsReader = inputFile.OpenRead())
                            {
                                using (StreamReader reader = new StreamReader(fsReader))
                                {
                                    WriteOutput(reader, skipRows, TRList, isFirstFile);
                                }
                            }
                            break;
                        case Enum_FileType.gzip:
                            using (FileStream fsReader = inputFile.OpenRead())
                            {
                                using (GZipInputStream unZipped = new ICSharpCode.SharpZipLib.GZip.GZipInputStream(fsReader))
                                {
                                    using (StreamReader reader = new StreamReader(unZipped))
                                    {
                                        WriteOutput(reader, skipRows, TRList, isFirstFile);
                                    }
                                }
                            }
                            break;
                        case Enum_FileType.bzip:
                            using (FileStream fsReader = inputFile.OpenRead())
                            {

                                using (BZip2InputStream unZipped = new ICSharpCode.SharpZipLib.BZip2.BZip2InputStream(fsReader))
                                {
                                    using (StreamReader reader = new StreamReader(unZipped))
                                    {
                                        WriteOutput(reader, skipRows, TRList, isFirstFile);
                                    }
                                }
                            }
                            break;
                        case Enum_FileType.lzw:
                            using (FileStream fsReader = inputFile.OpenRead())
                            {
                                using (LzwInputStream unZipped = new ICSharpCode.SharpZipLib.Lzw.LzwInputStream(fsReader))
                                {
                                    using (StreamReader reader = new StreamReader(unZipped))
                                    {
                                        WriteOutput(reader, skipRows, TRList, isFirstFile);
                                    }
                                }
                            }
                            break;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(String.Concat(@"Error: ", ex.Message));
                Console.WriteLine(@"");
                Console.WriteLine(ex.ToString());
            }
            foreach (TextWriter writer in TRList)
            {
                writer.Flush();
                writer.Close();
            }
        }

        private static int counter = 0;

        private static void WriteOutput(StreamReader reader, int skipRows, List<System.IO.TextWriter> TRList, bool ExtractHeader)
        {
            string? dataRow;
            int rowCounter = 0;
           
            while (!reader.EndOfStream)
            {
                dataRow = reader.ReadLine();
                if (rowCounter >= skipRows)
                {
                    if (dataRow != null)
                    {
                        if (!string.IsNullOrEmpty(dataRow.Trim()))
                        {
                            if (ExtractHeader)
                            {
                                ExtractHeader = false;
                                foreach (TextWriter writer in TRList)
                                {
                                    writer.WriteLine(dataRow);
                                }
                            }
                            else
                            {
                                if (counter >= TRList.Count)
                                {
                                    counter = 0;
                                }
                                TRList[counter].WriteLine(dataRow);
                                counter += 1;
                            }
                        }
                    }
                }
                rowCounter++;
            }
        }
    }
}
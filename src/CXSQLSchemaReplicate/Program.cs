using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Microsoft.SqlServer.Dac.Compare;
using Microsoft.SqlServer.Dac.Model;

namespace CXSQLSchemaReplicate
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string schemaDirectory = string.Empty;
            string[] FilesList = new string[0];
            bool IsParamsError;
            bool Param_PublishChanges = false;
            bool Param_GenScript = false;
            bool Param_PrintDifferences = false;
            IsParamsError = false;
            if (args.Length < 1)
            {
                IsParamsError = true;
            }
            if (!IsParamsError)
            {
                IsParamsError = true;
                schemaDirectory = args[0].Trim();
                if (System.IO.Directory.Exists(schemaDirectory))
                {
                    schemaDirectory = schemaDirectory.TrimEnd('\\');
                    FilesList = System.IO.Directory.GetFiles(string.Concat(schemaDirectory, @"\"), @"*.scmp");
                    if (FilesList.Length > 0)
                    {
                        IsParamsError = false;
                    }
                    else
                    {
                        Console.WriteLine(string.Concat(@"No .scmp files found in ", schemaDirectory));
                    }
                }
            }
            if (!IsParamsError)
            {
                if (args.Length > 1)
                {
                    if (args[1].ToLower().Trim() == @"y")
                    {
                        Param_PublishChanges = true;
                    }
                }
                if (args.Length > 2)
                {
                    if (args[2].ToLower().Trim() == @"y")
                    {
                        Param_GenScript = true;
                    }
                }
                if (args.Length > 3)
                {
                    if (args[3].ToLower().Trim() == @"y")
                    {
                        Param_PrintDifferences = true;
                    }
                }
            }
            if (IsParamsError)
            {
                Console.WriteLine(@"Usage: CXSQLSchemaReplicate.exe <<scmp folder path>> <<Publish Changes: Y/N>> <<GenScript: Y/N>> <<PrintDifference: Y/N>>");
                Console.WriteLine(@"Example: Publish Changes, Generate Script and Print Differences");
                Console.WriteLine(@"CXSQLSchemaReplicate.exe C:\SchemaCompare\ Y Y Y");
                Console.WriteLine();
                Console.WriteLine(@"Example: Publish Changes only");
                Console.WriteLine(@"Example: CXSQLSchemaReplicate.exe C:\SchemaCompare\ Y N N");
                Console.WriteLine();
                Console.WriteLine(@"Example: Publish Changes only and Print Differences");
                Console.WriteLine(@"Example: CXSQLSchemaReplicate.exe C:\SchemaCompare\ Y N Y");
                Console.WriteLine();
                Console.WriteLine(@"Example: Publish Changes only and Generate Script");
                Console.WriteLine(@"Example: CXSQLSchemaReplicate.exe C:\SchemaCompare\ Y Y N");
                Console.WriteLine();
                Console.WriteLine(@"Example: Generate Script and Print Differences but do NOT publish changes");
                Console.WriteLine(@"Example: CXSQLSchemaReplicate.exe C:\SchemaCompare\ N Y Y");
                return;
            }
            foreach (string file in FilesList)
            {
                if (File.Exists(file))
                {
                    try
                    {
                        SchemaComparison comparison = new SchemaComparison(file);
                        SchemaCompareDatabaseEndpoint EndPointSource;
                        SchemaCompareDatabaseEndpoint EndPointDestination;
                        EndPointSource = comparison.Source as SchemaCompareDatabaseEndpoint;
                        EndPointDestination = comparison.Target as SchemaCompareDatabaseEndpoint;
                        Console.WriteLine("Processing " + Path.GetFileName(file));
                        if (EndPointSource != null && EndPointDestination != null)
                        {
                            Console.WriteLine(string.Concat("Comparing schema: Source DB: ", EndPointSource.DatabaseName, @" - Target DB: ", EndPointDestination.DatabaseName));

                        }
                        else
                        {
                            Console.WriteLine("Comparing schema...");
                        }
                        SchemaComparisonResult comparisonResult = comparison.Compare();
                        if (Param_PrintDifferences)
                        {
                            int IndentLevel = 0;
                            StringBuilder sbDiff = new StringBuilder();
                            foreach (SchemaDifference Difference in comparisonResult.Differences)
                            {
                               
                                sbDiff.Append(SchemaDifferencePrintout(Difference));
                            }
                            Console.WriteLine(@"---------------------------------------");
                            Console.WriteLine(@"----------Schema Differences:----------");
                            Console.WriteLine(@"---------------------------------------");
                            Console.WriteLine(sbDiff.ToString());
                        }
                        if (Param_GenScript)
                        {
                            SchemaCompareScriptGenerationResult SQLScriptResult = comparisonResult.GenerateScript(EndPointDestination.DatabaseName);
                            if (SQLScriptResult.Success)
                            {
                                System.IO.File.WriteAllText(string.Concat(file, @".UpdateScript.sql"), SQLScriptResult.Script);
                                Console.WriteLine(string.Concat(@"Script file generated: ", file, @".UpdateScript.sql"));
                            }
                            else
                            {
                                Console.WriteLine(string.Concat(@"Script Generation Failed:", SQLScriptResult.Message));
                            }
                        }
                        if (Param_PublishChanges)
                        {
                            Console.WriteLine("Publishing schema...");
                            SchemaComparePublishResult publishResult = comparisonResult.PublishChangesToDatabase();
                        }
                    }
                    catch (Exception ex)
                    {
                    }
                }
            }
        }

        private static string SchemaDifferencePrintout(SchemaDifference Difference, int IndentLevel = 0)
        {
            StringBuilder sbDiff = new StringBuilder();
            if (IndentLevel > 0)
            {
                for (int i = 0; i < IndentLevel; i++)
                {
                    sbDiff.Append(@"-");
                }
                sbDiff.Append(@">");
            }
            TSqlObject SourceObject;
            SourceObject = Difference.SourceObject as TSqlObject;
            sbDiff.Append(Difference.Name).Append(@" ").Append(Enum.GetName(typeof(SchemaDifferenceType), Difference.DifferenceType)).Append(@" ")
                .Append(Difference.Included ? @"Included" : "Excluded").Append(@" ").Append(Enum.GetName(typeof(SchemaUpdateAction), Difference.UpdateAction));
            if (SourceObject  != null)
            {
                sbDiff.Append(@" ").Append(SourceObject.ObjectType.Name);
                if (SourceObject.Name.Parts.Count > 0)
                {
                    sbDiff.Append(@" ");
                    bool IsFirst = true;
                    foreach (string NamePart in SourceObject.Name.Parts)
                    {
                        if (IsFirst) { IsFirst = false; }
                        else { sbDiff.Append(@"."); }
                        sbDiff.Append(NamePart);
                    }
                }
            }
            sbDiff.AppendLine(string.Empty);
            foreach (SchemaDifference ChildDifference in Difference.Children)
            {
                sbDiff.Append(SchemaDifferencePrintout(ChildDifference, IndentLevel + 1));
            }
            return sbDiff.ToString();
        }
    }
}

using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Imosphere.Omop.ConnectedBradford.Helpers
{
    internal static class Generate
    {
        internal static string ReadResource(string name)
        {
            // Determine path
            var assembly = Assembly.GetExecutingAssembly();
            string resourcePath = name;

            resourcePath = assembly.GetManifestResourceNames()
                    .Single(str => str.EndsWith(name));

            using (Stream stream = assembly.GetManifestResourceStream(resourcePath))
            using (StreamReader reader = new StreamReader(stream))
            {
                return reader.ReadToEnd();
            }
        }

        internal static void WriteCreateAll(StringBuilder contents)
        {
            using (Stream stream = File.Create("..\\..\\..\\Helpers\\CreateAll.sql"))
            using (StreamWriter writer = new StreamWriter(stream))
            {
                writer.Write(contents);
            }
        }
    }
}

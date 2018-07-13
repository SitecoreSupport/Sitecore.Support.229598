using System.Collections.Concurrent;
using System.Collections.Generic;
using Sitecore.ContentSearch.Azure.Models;
using Sitecore.ContentSearch.Azure.Schema;

namespace Sitecore.Support.ContentSearch.Azure.Schema
{
  internal class CloudSearchIndexSchema : ICloudSearchIndexSchema
  {
    private readonly IDictionary<string, IndexedField> allFields;

    public CloudSearchIndexSchema(IEnumerable<IndexedField> fields)
    {
      this.allFields = new ConcurrentDictionary<string, IndexedField>();

      foreach (var indexedField in fields)
      {
        this.allFields.Add(indexedField.Name, indexedField);
      }
    }

    public ICollection<string> AllFieldNames => this.allFields.Keys;

    public IEnumerable<IndexedField> AllFields => this.allFields.Values;

    public IndexedField GetFieldByCloudName(string cloudName)
    {
      IndexedField field;

      if (!this.allFields.TryGetValue(cloudName, out field))
      {
        return null;
      }

      return field;
    }
  }
}
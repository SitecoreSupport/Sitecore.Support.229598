using System;
using Sitecore.ContentSearch.Azure.Exceptions;

namespace Sitecore.Support.ContentSearch.Azure.Http.Exceptions
{
  internal class NotFoundException : CloudSearchIndexException
  {
    public NotFoundException(string indexName, string message, Exception innerException)
      : base(indexName, message, innerException)
    {
    }
  }
}
using System;
using Sitecore.ContentSearch.Azure.Exceptions;

namespace Sitecore.Support.ContentSearch.Azure.Http.Exceptions
{
  internal class SearchServiceIsUnavailableException : CloudSearchIndexException
  {
    public SearchServiceIsUnavailableException(string indexName, string message, Exception innerException) : base(indexName, message, innerException)
    {
    }
  }
}
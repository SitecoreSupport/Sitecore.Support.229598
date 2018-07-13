namespace Sitecore.Support.ContentSearch.Azure.Http
{
  using System;
  using System.Linq;
  using System.Threading;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Azure;
  using Sitecore.ContentSearch.Azure.Http;
  using Sitecore.ContentSearch.Azure.Http.Exceptions;
  using Sitecore.ContentSearch.Azure.Models;
  using Sitecore.ContentSearch.Azure.Schema;
  using Sitecore.ContentSearch.Diagnostics;

  internal class SearchService : ISearchService, IProvideAvailabilityManager, ISearchServiceConnectionInitializable, ISearchIndexInitializable, IDisposable
  {
    private CloudSearchProviderIndex searchIndex;

    public SearchService(
        ISearchServiceAvailabilityManager availabilityManager,
        ISearchServiceDocumentOperationsProvider documentOperations,
        ISearchServiceSchemaSynchronizer schemaSynchronizer,
        string schemaUpdateInterval)
    {
      this.AvailabilityManager = availabilityManager;
      this.DocumentOperations = documentOperations;

      this.SchemaSynchronizer = schemaSynchronizer;
      // We need to be sure that schema changes done by another role are synced with others.
      this.timer = new Timer(this.SyncSchema, this, TimeSpan.FromSeconds(2), TimeSpan.Parse(schemaUpdateInterval));
    }

    private void SyncSchema(object state)
    {
      try
      {
        this.SchemaSynchronizer.RefreshLocalSchema();

        Interlocked.Exchange(ref schema, new Sitecore.Support.ContentSearch.Azure.Schema.CloudSearchIndexSchema(this.SchemaSynchronizer.LocalSchemaSnapshot.ToList()));
      }
      catch (Exception exception)
      {
        SearchLog.Log.Info("Schema synchronization failed", exception);
      }
    }

    public string Name { get; private set; }

    public ISearchServiceAvailabilityManager AvailabilityManager { get; set; }

    public ISearchServiceDocumentOperationsProvider DocumentOperations { get; set; }

    public ISearchServiceSchemaSynchronizer SchemaSynchronizer { get; set; }

    private Timer timer;
    private ICloudSearchIndexSchema schema;

    public ICloudSearchIndexSchema Schema
    {
      get { return this.schema; }
      private set { this.schema = value; }
    }

    public IndexStatistics GetStatistics()
    {
      return this.SchemaSynchronizer.ManagmentOperations.GetIndexStatistics();
    }

    public void PostDocuments(ICloudBatch batch)
    {
      try
      {
        this.PostDocumentsImpl(batch);
      }
      catch (Sitecore.Support.ContentSearch.Azure.Http.Exceptions.NotFoundException)
      {
        this.SchemaSynchronizer.RefreshLocalSchema();
        this.PostDocumentsImpl(batch);
      }
    }

    private void PostDocumentsImpl(ICloudBatch batch)
    {
      var schema = this.searchIndex.SchemaBuilder?.GetSchema();
      if (schema != null)
      {
        this.SchemaSynchronizer.EnsureIsInSync(schema.AllFields);
        this.Schema = new Sitecore.Support.ContentSearch.Azure.Schema.CloudSearchIndexSchema(this.SchemaSynchronizer.LocalSchemaSnapshot);
      }

      if (!this.AvailabilityManager.CanWrite)
      {
        var message = $"The service ${this.Name} is not available for write operations";

        throw new Sitecore.Support.ContentSearch.Azure.Http.Exceptions.SearchServiceIsUnavailableException(this.searchIndex.CloudIndexName, message, null);
      }

      var json = batch.GetJson();

      this.DocumentOperations.PostDocuments(json);
    }

    public string Search(string expression)
    {
      if (!this.AvailabilityManager.CanRead)
      {
        var message = $"The service ${this.Name} is not available for read operations";

        throw new Sitecore.Support.ContentSearch.Azure.Http.Exceptions.SearchServiceIsUnavailableException(this.searchIndex.CloudIndexName, message, null);
      }

      return this.DocumentOperations.Search(expression);
    }

    public void Cleanup()
    {
      if (this.SchemaSynchronizer.ManagmentOperations.IndexExists())
      {
        this.SchemaSynchronizer.ManagmentOperations.DeleteIndex();
      }

      this.SchemaSynchronizer.CleaupLocalSchema();
    }

    public virtual void Initialize(ISearchIndex index)
    {
      this.searchIndex = index as CloudSearchProviderIndex;

      if (this.searchIndex == null)
      {
        throw new NotSupportedException($"Only {typeof(CloudSearchProviderIndex).Name} is supported");
      }
      this.DocumentOperations.Observer = this.AvailabilityManager as IHttpMessageObserver;
      this.SchemaSynchronizer.ManagmentOperations.Observer = this.AvailabilityManager as IHttpMessageObserver;
    }

    public void Initialize(string indexName, string connectionString)
    {
      (this.DocumentOperations as ISearchServiceConnectionInitializable)?.Initialize(indexName, connectionString);
      (this.SchemaSynchronizer as ISearchServiceConnectionInitializable)?.Initialize(indexName, connectionString);

      var settings = new CloudSearchServiceSettings(connectionString);
      this.Name = settings.SearchService;

      this.SchemaSynchronizer.EnsureIsInitialized();
      this.Schema = new Sitecore.Support.ContentSearch.Azure.Schema.CloudSearchIndexSchema(this.SchemaSynchronizer.LocalSchemaSnapshot);
    }

    public void Dispose()
    {
      var timer = Interlocked.Exchange(ref this.timer, null);
      timer?.Dispose();
    }
  }
}
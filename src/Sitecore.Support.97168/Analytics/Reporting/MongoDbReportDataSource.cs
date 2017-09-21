namespace Sitecore.Support.Analytics.Reporting
{
  using System;
  using System.Configuration;
  using System.Data;

  using MongoDB.Bson;
  using MongoDB.Driver;

  using Sitecore.Analytics.Data;
  using Sitecore.Analytics.Data.DataAccess.MongoDb;
  using Sitecore.Analytics.Reporting.helper;
  using Sitecore.Diagnostics;
  using Sitecore.Analytics.Reporting;

  public class MongoDbReportDataSource : ReportDataSource
  {
    private const string COLLECTION_NAME_INTERACTIONS = "Interactions";
    private const string FIELD_NAME_CHANNEL_ID = "ChannelId";
    private const string FIELD_NAME_TRAFFIC_TYPE = "TrafficType";

    /// <summary>
    /// The driver to be used for MongoDb connections.
    /// </summary>
    private readonly MongoDbDriver driver;

    /// <summary>
    /// Initializes a new instance of the <see cref="MongoDbReportDataSource"/> class.
    /// </summary>
    /// <param name="connectionString">
    /// The connection string.
    /// </param>
    public MongoDbReportDataSource([NotNull] string connectionStringName)
    {
      Assert.ArgumentNotNull(connectionStringName, "connectionStringName");

      string connectionString = ConfigurationManager.ConnectionStrings[connectionStringName].ConnectionString;
      Assert.IsNotNull(connectionString, "connectionString");

      this.driver = new MongoDbDriver(connectionString);
    }



    /// <summary>
    /// Gets data from the MongoDB. 
    /// </summary>
    /// <param name="request">The request object containing the query, fields to return ect.</param>
    /// <returns>A <c>DataTable</c> object containing the retrieved data.</returns>
    public override DataTable GetData(ReportDataQuery request)
    {
      Assert.ArgumentNotNull(request, "request");
      QueryParser helper = this.GetQueryParser(request);

      string collection = helper.GetCollection();
      string[] fields = helper.GetFields();

      QueryDocument query = helper.GetQuery();

      foreach (var filterEntry in request.Filters)
      {
        var dataSourceFilter = this.GetFilter(filterEntry) as MongoDbValueListFilter;
        Assert.IsNotNull(dataSourceFilter, "dataSourceFilter");
        dataSourceFilter.Inject(query);
      }

      MongoCursor<BsonDocument> result = this.driver[collection].FindAs<BsonDocument>(query);

      //
      // If the ChannelId field is requested, make sure the TrafficType field is included as well.
      //

      bool includesInteractionChannelIdField = this.IncludesInteractionDocumentField(collection, fields, FIELD_NAME_CHANNEL_ID);
      bool includesInteractionTrafficTypeField = this.IncludesInteractionDocumentField(collection, fields, FIELD_NAME_TRAFFIC_TYPE);

      if ((includesInteractionChannelIdField) && (includesInteractionTrafficTypeField == false))
      {
        string[] extended = new string[fields.Length + 1];

        Array.Copy(fields, extended, fields.Length);

        extended[extended.Length - 1] = FIELD_NAME_TRAFFIC_TYPE;

        fields = extended;
      }

      result.SetFields(fields);

      SortByDocument sortBy = helper.GetSortBy();
      if (sortBy != null)
      {
        result.SetSortOrder(sortBy);
      }

      int skip = helper.GetSkip();
      result.Skip = skip;

      int limit = helper.GetLimit();
      if (limit != -1)
      {
        result.Limit = limit;
      }

      var table = new BsonDocumentLoader().GetDataTable(fields, result);
      table.TableName = collection + "Data";

      //
      // If the ChannelId field was requested, updated channel ID values if not set by
      // converting the value in the TrafficType field.
      //

      if (includesInteractionChannelIdField)
      {
        for (int i = 0; i < table.Rows.Count; i++)
        {
          Guid channelId = Guid.Empty;

          if (table.Rows[i][FIELD_NAME_CHANNEL_ID] != DBNull.Value)
          {
            channelId = (Guid)table.Rows[i][FIELD_NAME_CHANNEL_ID];
          }

          if ((channelId == Guid.Empty) && (table.Rows[i][FIELD_NAME_TRAFFIC_TYPE] != DBNull.Value))
          {
            int trafficType = (int)table.Rows[i][FIELD_NAME_TRAFFIC_TYPE];

            Guid? mappedChannelId = TrafficTypeConverter.ConvertToChannelId(trafficType);

            if (mappedChannelId != null)
            {
              table.Rows[i][FIELD_NAME_CHANNEL_ID] = mappedChannelId.Value;
            }
            else
            {
              table.Rows[i][FIELD_NAME_CHANNEL_ID] = Guid.Empty;
            }
          }
        }

        table.AcceptChanges();

        if (includesInteractionTrafficTypeField == false && table.Columns.Contains(FIELD_NAME_TRAFFIC_TYPE))
        {
          table.Columns.Remove(FIELD_NAME_TRAFFIC_TYPE);
        }
      }

      return table;
    }



    /// <summary>
    /// The get query parser.
    /// </summary>
    /// <param name="request">
    /// The request.
    /// </param>
    /// <returns>
    /// The Query parser<see cref="QueryParser"/>.
    /// </returns>
    [NotNull]
    protected virtual QueryParser GetQueryParser([NotNull] ReportDataQuery request)
    {
      Debug.ArgumentNotNull(request, "request");

      return new QueryParser(request.Query, request.Parameters);
    }


    /// <summary>
    ///   Determines if the specified collection and field set include the Interaction.ChannelId field.
    /// </summary>
    /// <param name="collection">
    ///   The name of the collection specified in the query.
    /// </param>
    /// <param name="fields">
    ///   The fields of the collection to return by the query.
    /// </param>
    /// <param name="field">
    ///   The name of the field to locate.
    /// </param>
    /// <returns>
    ///   <c>true</c> if the query includes the <i>Interaction.ChannelId</i> field; otherwise, <c>false</c>.
    /// </returns>
    private bool IncludesInteractionDocumentField([NotNull] string collection, [NotNull] string[] fields, string field)
    {
      Debug.ArgumentNotNull(collection, "collection");
      Debug.ArgumentNotNull(fields, "fields");
      Debug.ArgumentNotNull(field, "field");

      bool result = false;

      bool isInteractionsCollection = string.Equals(collection, COLLECTION_NAME_INTERACTIONS, StringComparison.Ordinal);

      if (isInteractionsCollection)
      {
        for (int i = 0; i < fields.Length; i++)
        {
          result = string.Equals(fields[i], field, StringComparison.Ordinal);

          if (result)
          {
            break;
          }
        }
      }

      return result;
    }
  }
}


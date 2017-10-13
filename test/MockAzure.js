'use strict';

const nock = require('nock');

let entityCount = 0;

module.exports = MockAzure;

function MockAzure(tableName) {
  tableName = tableName || 'testtableclient';
  const baseUrl = `http://${tableName}ns.table.core.windows.local`;
  const connectionString = `DefaultEndpointsProtocol=http;AccountName=${tableName}ns;AccountKey=YXBwYXBwYXBw;EndpointSuffix=core.windows.local`;

  const scope = nock(baseUrl);

  return {
    connectionString,
    tableName,
    reset,
    batchPayload,
    connection,
    createResponseEntity,
    deleteTableIfExists,
    errorResponse,
    executeBatch,
    isComplete,
    tableBeingDeletedError,
    tableNotFound,
    queryEntities,
    queryFailed,
    queryResponse,
  };

  function reset() {
    nock.cleanAll();
  }

  function isComplete() {
    return nock.isDone();
  }

  function connection() {
    scope
      .get(`/Tables(%27${tableName}%27)`)
      .reply(200, {
        'odata.metadata': `${baseUrl}/$metadata#Tables/@Element`,
        'TableName': tableName
      }, replyHeaders());
  }

  function tableNotFound() {
    const headers = replyHeaders();
    scope
      .get(`/Tables(%27${tableName}%27)`)
      .reply(404, notFoundPayload(headers), headers);
  }

  function queryEntities(statusCode = 200, payload) {
    const headers = replyHeaders();
    scope
      .get(`/${tableName}`)
      .query(true)
      .reply(statusCode, payload, headers);
  }

  function queryFailed(statusCode = 404, payload) {
    const headers = replyHeaders();
    if (payload === undefined) payload = notFoundPayload(headers);
    return queryEntities(statusCode, payload);
  }

  function errorResponse(code = 'TableBeingDeleted', message = 'storage error') {
    return {
      'odata.error': {
        code,
        'message': {
          'lang': 'en-US',
          'value': message
        }
      }
    };
  }

  function tableBeingDeletedError() {
    const headers = replyHeaders();
    scope
      .post('/Tables', {'TableName': tableName})
      .reply(409, {
        'odata.error': {
          'code': 'TableBeingDeleted',
          'message': {
            'lang': 'en-US',
            'value': `The specified table is being deleted. Try operation later.\nRequestId:${headers['x-ms-request-id']}\nTime:${new Date().toJSON()}`
          }
        }
      }, headers);
  }

  function replyHeaders(override) {
    return Object.assign({
      'Content-Type': 'application/json;odata=minimalmetadata;streaming=true;charset=utf-8',
      'x-ms-request-id': '00000000-0000-0000-0000-000000000000',
      'x-ms-version': '2017-04-17',
    }, override);
  }

  function notFoundPayload(headers) {
    return {
      'odata.error': {
        'code': 'ResourceNotFound',
        'message': {
          'lang': 'en-US',
          'value': `The specified resource does not exist.\nRequestId:${headers['x-ms-request-id']}\nTime:${new Date().toJSON()}`
        }
      }
    };
  }

  function queryResponse(value) {
    return {
      'odata.metadata': `${baseUrl}/$metadata#${tableName}`,
      value: value || [createResponseEntity()]
    };
  }

  function createResponseEntity(override) {
    const date = new Date();
    const Timestamp = date.toJSON();
    return Object.assign({
      'odata.etag': `W/datetime'${encodeURIComponent(Timestamp)}'`,
      PartitionKey: 'segment',
      RowKey: `test${entityCount++}`,
      Timestamp,
      gc: true,
      item: '{}',
      ttl: 0,
      'ttl_int@odata.type': 'Edm.Int64',
      ttl_int: date.getTime().toString()
    }, override);
  }

  function executeBatch(statusCode = 202, response) {
    const headers = replyHeaders(statusCode === 202 ? {
      'Content-Type': 'multipart/mixed; boundary=batchresponse_1'
    } : {});
    response = response || batchResponse();
    scope
      .post('/$batch')
      .reply(statusCode, response, headers);
  }

  function deleteTableIfExists(statusCode = 201, response) {
    const headers = replyHeaders();
    scope
      .delete(`/Tables(%27${tableName}%27)`)
      .reply(statusCode, response, headers);
  }

  function batchPayload() {
    return `--batch_1
content-type: multipart/mixed;charset="utf-8";boundary=changeset_1

--changeset_1
content-type: application/http
content-transfer-encoding: binary

DELETE ${baseUrl}:443/${tableName}(PartitionKey=%27segment%27,RowKey=%27test%27) HTTP/1.1
if-match: W/datetime'2017-10-11T04%3A22%3A20.3892507Z'
accept: application/json;odata=minimalmetadata
maxdataserviceversion: 3.0;NetFx


--changeset_1--
--batch_1--`;
  }

  function batchResponse() {
    return `--batchresponse_1
Content-Type: multipart/mixed; boundary=changesetresponse_1

--changesetresponse_1
Content-Type: application/http
Content-Transfer-Encoding: binary

HTTP/1.1 204 No Content
X-Content-Type-Options: nosniff
Cache-Control: no-cache
DataServiceVersion: 1.0;

--changesetresponse_1--
--batchresponse_1--`;
  }
}

var AWS = require("aws-sdk");


AWS.config.update({
    region: "eu-central-1",
});

var dynamodb = new AWS.DynamoDB.DocumentClient();
var sourceTable = "asset-records-production";
var targetTable = "asset-records-testing";
var assetPrefix = "urn:teltonika::asset/"
var batchRequest = [];

var indexName = "asset-recorded-index";
var startDate = new Date();
var endDate = new Date();
startDate.setHours(0, 0, 0, 0);
endDate.setHours(23, 59, 59, 999);

//const assets = ["urn:teltonika::asset/358480081835169"]
main();

async function main() {
    console.log('Start')
    var assetsData = await getAssetsFromTable();
    for (let index = 0; index < assetsData.length; index++) {
        const assetData = assetsData[index];
        console.log(assetData["asset"]);
        var fetchedItems;
        if(assetData["data_provider"] == "munic"){
            sourceTable = "asset-records-munic-production";
            targetTable = "asset-records-munic-staging";
            assetPrefix = "urn:munic::asset/";
            fetchedItems = await loadProductionRecords(assetData);
        }
        else{
            sourceTable = "asset-records-production";
            targetTable = "asset-records-testing";
            assetPrefix = "urn:teltonika::asset/"
            fetchedItems = await loadProductionRecords(assetData);
        }
        if(fetchedItems.Count == 0){
            console.log("No Items found.")
            return;
        }
        await prepareBatchRequestAndInsert(fetchedItems);
    }
}
async function getAssetsFromTable(){
    AWS.config.update({
        region: "eu-central-1",
        endpoint: 'http://localhost:8000'
      });
      
      var docClient = new AWS.DynamoDB.DocumentClient();
      
        const params = {
            TableName: "asset-staging-insert",
        };
    
        let scanResults = [];
        let items;
        do{
            items =  await docClient.scan(params).promise();
            items.Items.forEach((item) => scanResults.push(item));
            params.ExclusiveStartKey  = items.LastEvaluatedKey;
        }while(typeof items.LastEvaluatedKey != "undefined");
    
        return scanResults;
    

}
async function prepareBatchRequestAndInsert(fetchedItems) {
    Object.keys(fetchedItems).forEach(function (key) {
        if (key == "Items") {
            var items = fetchedItems[key];
            
            items.forEach( element => {
                console.log("items", typeof(element.world_coordinates));

              batchRequest.push({
                        PutRequest:{
                            Item: {
                                "id":{
                                    S: element.id
                                },
                               "asset":{
                                   S: element.asset
                               },
                               "data_provider":{
                                   S: element.data_provider
                               },
                               "odometer":{
                                   S: element.odometer
                               },
                               "odometer_source":{
                                   S: element.odometer_source
                               },
                               "recorded":{
                                   S: element.recorded
                               },
                               "trip_events":{
                                   S: element.trip_events
                               },
                               "world_coordinates":{
                                   S: element.world_coordinates
                               }
                           }
                        }
               });
               removeEmptyStringElements(batchRequest[batchRequest.length - 1].PutRequest.Item)
              
            });
            var i,j,tempBatchRequest,chunk = 25;
            for (i=0,j=batchRequest.length; i<j; i+=chunk) {
                tempBatchRequest = batchRequest.slice(i,i+chunk);
                console.log("legnth:" +tempBatchRequest.length)
                if(tempBatchRequest.length > 0)
                    var response =  insetBatchStaging(tempBatchRequest);
            } 
           
            batchRequest = [];
        }
    });
   

}
async function removeEmptyStringElements(obj) {
    for (var prop in obj) {
     if(obj[prop].S === undefined || obj[prop].S === null) {// delete elements that are empty strings
        delete obj[prop];
      }
    }
    return obj;
  }
async function loadProductionRecords(assetData) {
    try {
        var params = {
            IndexName: indexName,
            TableName: sourceTable,
            Select: 'ALL_ATTRIBUTES',
            ScanIndexForward: true,
            KeyConditionExpression: 'asset = :asset and recorded between :startDate and :endDate',
            ExpressionAttributeValues: {
                ':asset': assetPrefix + assetData["asset"],
                ':startDate': '2019-11-12T00:00:00.000Z',//startDate.toISOString(),
                ':endDate': '2019-11-12T23:59:59.000Z'//endDate.toISOString()
            }
        };
        return await dynamodb.query(params).promise()
    } catch (error) {
        console.error(error);
    }
}

async function loadProductionRecordsMunic(assetData) {
    try {
        var params = {
            TableName: sourceTable,
            TableName: sourceTable,
            Select: 'ALL_ATTRIBUTES',
            ScanIndexForward: true,
            ExpressionAttributeValues: {
                ':asset': assetPrefix + assetData["asset"],
                ':startDate': '2019-11-12T00:00:00.000Z',//startDate.toISOString(),
                ':endDate': '2019-11-12T23:59:59.000Z'//endDate.toISOString()
            },
            FilterExpression: [
                'asset = :asset AND recorded between :startDate and :endDate' //startDate.toISOString(),
            ]
        };
        return await dynamodb.scan(params).promise()
    } catch (error) {
        console.error(error);
    }
}


async function insetBatchStaging(batchRequest) {
    console.log("Insert Started")
    AWS.config.update({
        region: "eu-central-1",
        endpoint: 'http://localhost:8000'
      });
      
    
    var params = {
        RequestItems: {
            [targetTable] : batchRequest
            }
            };
    var dynamodbLocal = new AWS.DynamoDB();
   
    return dynamodbLocal.batchWriteItem(params, function (err, data) {
        console.log("Batch Write")
        console.log("Error:" + err);
        console.log(null, data);
           
    });


}

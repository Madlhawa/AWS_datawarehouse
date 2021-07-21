import json
import pandas as pd    

response = {
    "Items":[
       {
          "Address":{
             "AddressLine2":{
                "Street":"12,Berkly Lane",
                "PostalCode":"90012",
                "City":"Los Angeles",
                "StateProvinceName":"California"
             },
             "AddressLine1":{
                "Street":"5781 Sharon Dr.",
                "PostalCode":"90012",
                "City":"Los Angeles",
                "StateProvinceName":"California"
             }
          },
          "Country":"United States",
          "CustomerID":"11018",
          "AddressType":"Home"
       },
       {
          "Address":{
             "AddressLine2":{
                "Street":"NULL",
                "PostalCode":"NULL",
                "City":"NULL",
                "StateProvinceName":"NULL"
             },
             "AddressLine1":{
                "Street":"7297 Kaywood Drive",
                "PostalCode":"4169",
                "City":"East Brisbane",
                "StateProvinceName":"Queensland"
             }
          },
          "Country":"Australia",
          "CustomerID":"11020",
          "AddressType":"Home"
       },
       {
          "Address":{
             "AddressLine2":{
                "Street":"NULL",
                "PostalCode":"NULL",
                "City":"NULL",
                "StateProvinceName":"NULL"
             },
             "AddressLine1":{
                "Street":"3791 Rossmor Parkway",
                "PostalCode":"91910",
                "City":"Chula Vista",
                "StateProvinceName":"California"
             }
          },
          "Country":"United States",
          "CustomerID":"11015",
          "AddressType":"Home"
       },
       {
          "Address":{
             "AddressLine2":{
                "Street":"NULL",
                "PostalCode":"NULL",
                "City":"NULL",
                "StateProvinceName":"NULL"
             },
             "AddressLine1":{
                "Street":"8668 St. Celestine Court",
                "PostalCode":"97355",
                "City":"Lebanon",
                "StateProvinceName":"Oregon"
             }
          },
          "Country":"United States",
          "CustomerID":"11012",
          "AddressType":"Home"
       },
       {
          "Address":{
             "AddressLine2":{
                "Street":"Wales Street",
                "PostalCode":"92102",
                "City":"San Diego",
                "StateProvinceName":"California"
             },
             "AddressLine1":{
                "Street":"4308 Sand Pointe Lane",
                "PostalCode":"92102",
                "City":"San Diego",
                "StateProvinceName":"California"
             }
          },
          "Country":"United States",
          "CustomerID":"11016",
          "AddressType":"Home"
       },
       {
          "Address":{
             "AddressLine2":{
                "Street":"NULL",
                "PostalCode":"NULL",
                "City":"NULL",
                "StateProvinceName":"NULL"
             },
             "AddressLine1":{
                "Street":"6083 San Jose",
                "PostalCode":"V2W 1W2",
                "City":"Haney",
                "StateProvinceName":"British Columbia"
             }
          },
          "Country":"Canada",
          "CustomerID":"11019",
          "AddressType":"Home"
       },
       {
          "Address":{
             "AddressLine2":{
                "Street":"NULL",
                "PostalCode":"NULL",
                "City":"NULL",
                "StateProvinceName":"NULL"
             },
             "AddressLine1":{
                "Street":"2685 Blackburn Ct",
                "PostalCode":"90802",
                "City":"Long Beach",
                "StateProvinceName":"California"
             }
          },
          "Country":"United States",
          "CustomerID":"11017",
          "AddressType":"Home"
       },
       {
          "Address":{
             "AddressLine2":{
                "Street":"NULL",
                "PostalCode":"NULL",
                "City":"NULL",
                "StateProvinceName":"NULL"
             },
             "AddressLine1":{
                "Street":"867 La Orinda Place",
                "PostalCode":"91950",
                "City":"Lincoln Acres",
                "StateProvinceName":"California"
             }
          },
          "Country":"United States",
          "CustomerID":"11011",
          "AddressType":"Home"
       },
       {
          "Address":{
             "AddressLine2":{
                "Street":"24,Ward street",
                "PostalCode":"92625",
                "City":"Newport Beach",
                "StateProvinceName":"California"
             },
             "AddressLine1":{
                "Street":"7926 Stephanie Way",
                "PostalCode":"92625",
                "City":"Newport Beach",
                "StateProvinceName":"California"
             }
          },
          "Country":"United States",
          "CustomerID":"11013",
          "AddressType":"Home"
       },
       {
          "Address":{
             "AddressLine2":{
                "Street":"NULL",
                "PostalCode":"NULL",
                "City":"NULL",
                "StateProvinceName":"NULL"
             },
             "AddressLine1":{
                "Street":"2939 Wesley Ct.",
                "PostalCode":"90401",
                "City":"Santa Monica",
                "StateProvinceName":"California"
             }
          },
          "Country":"United States",
          "CustomerID":"11014",
          "AddressType":"Home"
       }
    ],
    "Count":10,
    "ScannedCount":10,
    "ResponseMetadata":{
       "RequestId":"CM2OJ1DFCJ7D0B1D5CQ8SPMDEJVV4KQNSO5AEMVJF66Q9ASUAAJG",
       "HTTPStatusCode":200,
       "HTTPHeaders":{
          "server":"Server",
          "date":"Wed, 21 Jul 2021 11:43:13 GMT",
          "content-type":"application/x-amz-json-1.0",
          "content-length":"3916",
          "connection":"keep-alive",
          "x-amzn-requestid":"CM2OJ1DFCJ7D0B1D5CQ8SPMDEJVV4KQNSO5AEMVJF66Q9ASUAAJG",
          "x-amz-crc32":"841858796"
       },
       "RetryAttempts":0
    }
 }

response = json.dumps(response)
address_json = json.loads(response)
address_dataframe = pd.json_normalize(address_json['Items'], max_level=1)

address1_dataframe = address_dataframe.drop('Address.AddressLine2', axis=1).rename({'Address.AddressLine1':'Address'},axis=1)
address2_dataframe = address_dataframe.drop('Address.AddressLine1', axis=1).rename({'Address.AddressLine2':'Address'},axis=1)

address_dataframe = address1_dataframe.append(address2_dataframe, ignore_index=True)

address_dataframe = address_dataframe.join(address_dataframe['Address'].apply(json.dumps).apply(json.loads).apply(pd.Series)).drop('Address', axis=1)
address_dataframe = address_dataframe[address_dataframe.eval('Street != "NULL" & PostalCode != "NULL" & City != "NULL" & StateProvinceName != "NULL"')]
print(address_dataframe)

address_dataframe.to_csv('./dataset-sent/address.csv')
// Everything the project needs, in one resource group.
//   az group create -n rg-flightlake -l westus2
//   az deployment group create -g rg-flightlake -f infra/main.bicep -p prefix=flk
// Delete the resource group when finished: az group delete -n rg-flightlake

@description('Short lowercase prefix, 3-8 letters/digits. Used in every resource name.')
@minLength(3)
@maxLength(8)
param prefix string = 'flk'

param location string = resourceGroup().location

@description('premium is required for Unity Catalog. trial = 14 days of premium with no Databricks (DBU) charge; VMs still bill.')
@allowed(['premium', 'trial'])
param databricksSku string = 'premium'

@description('Set to false to skip (or later remove) Event Hubs, the only resource here that bills while idle.')
param deployEventHub bool = true

var suffix = uniqueString(resourceGroup().id)
var storageName = toLower(take('st${prefix}${suffix}', 24))
var blobDataContributor = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')

// ---------------------------------------------------------------- ADLS Gen2
resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true // hierarchical namespace = Data Lake Storage Gen2
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource blob 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = [
  for name in ['landing', 'lake']: {
    parent: blob
    name: name
  }
]

// ---------------------------------------------------------------- Data Factory
resource adf 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: 'adf-${prefix}-${suffix}'
  location: location
  identity: { type: 'SystemAssigned' }
}

// Data Factory writes to the lake with its managed identity. No keys anywhere.
resource adfLakeAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: storage
  name: guid(storage.id, adf.id, blobDataContributor)
  properties: {
    roleDefinitionId: blobDataContributor
    principalId: adf.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// ---------------------------------------------------------------- Databricks
resource accessConnector 'Microsoft.Databricks/accessConnectors@2023-05-01' = {
  name: 'dbac-${prefix}-${suffix}'
  location: location
  identity: { type: 'SystemAssigned' }
}

// Unity Catalog reaches the lake through this connector's managed identity.
resource connectorLakeAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: storage
  name: guid(storage.id, accessConnector.id, blobDataContributor)
  properties: {
    roleDefinitionId: blobDataContributor
    principalId: accessConnector.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource databricks 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: 'dbw-${prefix}-${suffix}'
  location: location
  sku: { name: databricksSku }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', '${resourceGroup().name}-dbx-managed')
  }
}

// ---------------------------------------------------------------- Event Hubs
// Standard tier: the Kafka endpoint used by Spark is not available on Basic.
resource ehNamespace 'Microsoft.EventHub/namespaces@2021-11-01' = if (deployEventHub) {
  name: 'evhns-${prefix}-${suffix}'
  location: location
  sku: { name: 'Standard', tier: 'Standard', capacity: 1 }
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2021-11-01' = if (deployEventHub) {
  parent: ehNamespace
  name: 'flight-events'
  properties: { partitionCount: 2, messageRetentionInDays: 1 }
}

resource ehRule 'Microsoft.EventHub/namespaces/eventhubs/authorizationRules@2021-11-01' = if (deployEventHub) {
  parent: eventHub
  name: 'send-listen'
  properties: { rights: ['Send', 'Listen'] }
}

output storageAccountName string = storage.name
output dataFactoryName string = adf.name
output databricksWorkspaceUrl string = 'https://${databricks.properties.workspaceUrl}'
output accessConnectorId string = accessConnector.id
output eventHubNamespace string = deployEventHub ? ehNamespace.name : ''

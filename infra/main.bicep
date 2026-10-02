// NFL Azure Lab - infrastructure as code
// Deploys: ADLS Gen2 storage (bronze + config containers), Key Vault, Data Factory,
// and the RBAC role assignments that let ADF write to the lake and Fabric read it.

@description('Environment name. Same template, different parameter file per environment.')
@allowed([
  'dev'
  'prod'
])
param env string = 'dev'

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Object ID of the Entra user you sign into Fabric with. Grants read access to the lake for the Fabric shortcut. Leave empty to skip.')
param fabricUserObjectId string = ''

// uniqueString() is deterministic per resource group, so names stay stable across redeploys
var suffix = uniqueString(resourceGroup().id)
var storageName = 'stnfl${env}${suffix}'
var keyVaultName = 'kv-nfl-${env}-${take(suffix, 8)}'
var dataFactoryName = 'adf-nfl-${env}-${suffix}'
var tags = {
  project: 'nfl-lab'
  env: env
  owner: 'tristan'
}

// Built-in Azure role definition IDs
var storageBlobDataContributor = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
var storageBlobDataReader = '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'
var keyVaultSecretsUser = '4633458b-17de-408a-b874-0445c86b69e6'

// ---------- Data lake (ADLS Gen2 = storage account with hierarchical namespace) ----------
resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    accessTier: 'Hot'
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource containers 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = [
  for name in [
    'bronze'
    'config'
  ]: {
    parent: blobService
    name: name
  }
]

// ---------- Key Vault (RBAC mode, no access policies) ----------
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    tenantId: subscription().tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 7
  }
}

// ---------- Data Factory with a system-assigned managed identity ----------
resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
  properties: {}
}

// ---------- RBAC: least-privilege access, no keys or passwords anywhere ----------

// ADF can read/write the lake
resource adfLakeAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, dataFactory.id, storageBlobDataContributor)
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributor)
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// ADF can read secrets (e.g. a source system password) from Key Vault
resource adfKeyVaultAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(keyVault.id, dataFactory.id, keyVaultSecretsUser)
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', keyVaultSecretsUser)
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// Your Fabric user can read the lake (for the OneLake shortcut)
resource fabricLakeRead 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (!empty(fabricUserObjectId)) {
  name: guid(storage.id, fabricUserObjectId, storageBlobDataReader)
  scope: storage
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataReader)
    principalId: fabricUserObjectId
    principalType: 'User'
  }
}

output storageAccountName string = storage.name
output dfsEndpoint string = storage.properties.primaryEndpoints.dfs
output keyVaultName string = keyVault.name
output dataFactoryName string = dataFactory.name

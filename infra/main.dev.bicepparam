using 'main.bicep'

// Dev environment. A prod file would look identical with env = 'prod'.
param env = 'dev'
param location = 'eastus'

// Set this to the object ID of your Fabric user (see README).
param fabricUserObjectId = ''

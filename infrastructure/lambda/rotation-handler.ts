import { GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';
import { RotationEvent, smClient, RotationStrategy } from './common.js';
import { AwsApiKeyStrategy } from './strategies/aws-api-key.js';
import { RdsMysqlStrategy } from './strategies/mysql.js';
import { RdsPostgresStrategy } from './strategies/postgres.js';
import { DocumentDbStrategy } from './strategies/documentdb.js';
import { GenericStrategy } from './strategies/generic.js';

export async function handler(event: RotationEvent): Promise<void> {
  console.log('Rotation event received:', JSON.stringify(event, null, 2));
  const { Step, SecretId, ClientRequestToken } = event;

  try {
    const strategy = await getStrategy(SecretId, ClientRequestToken, Step);

    // We need to fetch the secret data again for the strategy methods?
    // Strategies expect (secretId, token, dict). 
    // My interface was: createSecret(secretId, token, currentDict).
    // So I should pass the loaded dictionaries to the strategy methods?
    // Refactoring the interface slightly in my head: 
    // The previous implementation fetched secret inside each step.
    // The strategies I wrote above also fetch secret inside or expect dict?
    // Let's check my strategies code.
    // My strategies `createSecret` calls `smClient` to put value, but assumes `currentDict` is passed in?
    // Wait, let's check `mysql.ts`:
    // `async createSecret(secretId: string, token: string, currentDict: any)`
    // It uses `currentDict` directly.
    // So `handler` needs to prepare `currentDict` (and maybe `pendingDict` for other steps).

    // Fetch current secret for context
    const currentSecret = await smClient.send(
      new GetSecretValueCommand({ SecretId: SecretId, VersionStage: 'AWSCURRENT' })
    );
    const currentDict = JSON.parse(currentSecret.SecretString || '{}');

    // For set/test, we also need pendingDict
    let pendingDict: any = {};
    if (Step === 'setSecret' || Step === 'testSecret') {
      const pendingSecret = await smClient.send(
        new GetSecretValueCommand({ SecretId: SecretId, VersionId: ClientRequestToken, VersionStage: 'AWSPENDING' })
      );
      pendingDict = JSON.parse(pendingSecret.SecretString || '{}');
    }

    switch (Step) {
      case 'createSecret':
        // Check if AWSPENDING already exists
        try {
          await smClient.send(
            new GetSecretValueCommand({ SecretId: SecretId, VersionId: ClientRequestToken, VersionStage: 'AWSPENDING' })
          );
          console.log('AWSPENDING already exists.');
          return;
        } catch (e: any) {
          if (e.name !== 'ResourceNotFoundException') throw e;
        }
        await strategy.createSecret(SecretId, ClientRequestToken, currentDict);
        break;
      case 'setSecret':
        await strategy.setSecret(SecretId, ClientRequestToken, pendingDict, currentDict);
        break;
      case 'testSecret':
        await strategy.testSecret(SecretId, ClientRequestToken, pendingDict);
        break;
      case 'finishSecret':
        await strategy.finishSecret(SecretId, ClientRequestToken, currentSecret);
        break;
      default:
        throw new Error(`Unknown step: ${Step}`);
    }
  } catch (error) {
    console.error(`Error in step ${Step}:`, error);
    throw error;
  }
}

async function getStrategy(secretId: string, token: string, step: string): Promise<RotationStrategy> {
  // Strategy determination relies on the secret type.
  // We usually check AWSCURRENT to determine type.
  // But if we are in setSecret/testSecret, AWSPENDING might have the type info too.
  // Assuming type doesn't change during rotation (it shouldn't).

  const currentSecret = await smClient.send(
    new GetSecretValueCommand({ SecretId: secretId, VersionStage: 'AWSCURRENT' })
  );
  const dict = JSON.parse(currentSecret.SecretString || '{}');
  const type = dict.nightwatch_secret_type;

  if (type === 'AWS_API_KEY') {
    return new AwsApiKeyStrategy();
  } else if (type === 'RDS_CREDENTIALS') {
    if (dict.engine === 'postgres') return new RdsPostgresStrategy();
    return new RdsMysqlStrategy(); // default or mysql
  } else if (type === 'DOCUMENTDB_CREDENTIALS') {
    return new DocumentDbStrategy();
  } else {
    return new GenericStrategy();
  }
}

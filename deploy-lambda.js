/**
 * deploy-lambda.js — package + deploy the AWS-native CC extraction Lambda (Phase 2).
 * Zips ./lambda-build, ensures the S3 output bucket + IAM execution role, then creates/updates the
 * `cc-extract` function. Idempotent — safe to re-run after code changes. Uses local ~/.aws creds.
 *
 *   node deploy-lambda.js            # build + deploy (create or update)
 *   node deploy-lambda.js --code     # update function code only (fast redeploy)
 */
const path = require('path');
const AdmZip = require('adm-zip');
const { STSClient, GetCallerIdentityCommand } = require('@aws-sdk/client-sts');
const { IAMClient, CreateRoleCommand, PutRolePolicyCommand, GetRoleCommand } = require('@aws-sdk/client-iam');
const { S3Client, CreateBucketCommand, HeadBucketCommand } = require('@aws-sdk/client-s3');
const {
  LambdaClient, CreateFunctionCommand, GetFunctionCommand,
  UpdateFunctionCodeCommand, UpdateFunctionConfigurationCommand,
} = require('@aws-sdk/client-lambda');

const REGION = process.env.AWS_REGION || 'us-east-1';
const FN = 'cc-extract';
const ROLE = 'rampedup-cc-extract-lambda';
const sts = new STSClient({ region: REGION });
const iam = new IAMClient({ region: REGION });
const s3 = new S3Client({ region: REGION });
const lambda = new LambdaClient({ region: REGION });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function ensureBucket(bucket) {
  try { await s3.send(new HeadBucketCommand({ Bucket: bucket })); console.error(`  bucket ${bucket} exists`); return; }
  catch (_) { /* create below */ }
  await s3.send(new CreateBucketCommand({ Bucket: bucket }));   // us-east-1: no LocationConstraint
  console.error(`  bucket ${bucket} created`);
}

async function ensureRole(bucket) {
  const trust = { Version: '2012-10-17', Statement: [{ Effect: 'Allow', Principal: { Service: 'lambda.amazonaws.com' }, Action: 'sts:AssumeRole' }] };
  const policy = { Version: '2012-10-17', Statement: [
    { Effect: 'Allow', Action: 's3:GetObject', Resource: 'arn:aws:s3:::commoncrawl/*' },
    { Effect: 'Allow', Action: ['s3:PutObject', 's3:GetObject', 's3:ListBucket'], Resource: [`arn:aws:s3:::${bucket}`, `arn:aws:s3:::${bucket}/*`] },
    { Effect: 'Allow', Action: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'], Resource: 'arn:aws:logs:*:*:*' },
  ] };
  let arn;
  try { arn = (await iam.send(new GetRoleCommand({ RoleName: ROLE }))).Role.Arn; console.error(`  role ${ROLE} exists`); }
  catch (_) {
    arn = (await iam.send(new CreateRoleCommand({ RoleName: ROLE, AssumeRolePolicyDocument: JSON.stringify(trust) }))).Role.Arn;
    console.error(`  role ${ROLE} created — waiting for propagation…`);
    await sleep(12000);
  }
  await iam.send(new PutRolePolicyCommand({ RoleName: ROLE, PolicyName: 'cc-extract-policy', PolicyDocument: JSON.stringify(policy) }));
  return arn;
}

(async () => {
  const acct = (await sts.send(new GetCallerIdentityCommand({}))).Account;
  const bucket = process.env.OUT_BUCKET || `rampedup-cc-extracted-${acct}`;
  console.error(`account ${acct}, region ${REGION}, out-bucket ${bucket}`);

  console.error('zipping ./lambda-build …');
  const zip = new AdmZip();
  zip.addLocalFolder(path.join(__dirname, 'lambda-build'));
  const code = zip.toBuffer();
  console.error(`  zip size ${(code.length / 1048576).toFixed(1)} MB`);

  if (process.argv.includes('--code')) {
    await lambda.send(new UpdateFunctionCodeCommand({ FunctionName: FN, ZipFile: code }));
    console.error(`updated ${FN} code only.`); return;
  }

  await ensureBucket(bucket);
  const roleArn = await ensureRole(bucket);

  const cfg = {
    FunctionName: FN, Runtime: 'nodejs20.x', Role: roleArn, Handler: 'lambda-extract.handler',
    Timeout: 180, MemorySize: 1024, Environment: { Variables: { OUT_BUCKET: bucket } },
  };
  let exists = false;
  try { await lambda.send(new GetFunctionCommand({ FunctionName: FN })); exists = true; } catch (_) { /* create */ }
  if (exists) {
    await lambda.send(new UpdateFunctionCodeCommand({ FunctionName: FN, ZipFile: code }));
    await sleep(3000);
    await lambda.send(new UpdateFunctionConfigurationCommand(cfg));
    console.error(`updated ${FN}.`);
  } else {
    for (let attempt = 1; ; attempt++) {
      try { await lambda.send(new CreateFunctionCommand({ ...cfg, Code: { ZipFile: code } })); break; }
      catch (e) {
        // The freshly-created role may not be assumable yet — retry a few times.
        if (attempt < 5 && /cannot be assumed|not authorized to perform: sts:AssumeRole/i.test(e.message || '')) {
          console.error(`  role not ready (attempt ${attempt}); waiting…`); await sleep(6000); continue;
        }
        throw e;
      }
    }
    console.error(`created ${FN}.`);
  }
  console.error(`\nDONE. Test: node -e "invoke ${FN} with {pointers:[…]}"  |  OUT_BUCKET=${bucket}`);
})().catch((e) => { console.error('deploy error:', e.message); process.exit(1); });

/*
 * This script synchronizes the local DynamoDB server with the tables defined in the SAM template.
 * It lists the tables in the local DynamoDB server, compares them with the tables defined in the
 * SAM template, and creates, updates, or deletes tables as needed.
 */

import path from "node:path";
import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
  ListTablesCommand,
  UpdateTableCommand,
} from "@aws-sdk/client-dynamodb";
import fs from "fs-extra";
import isEqual from "lodash.isequal";
import { yamlParse } from "yaml-cfn";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const retry = async (count, delay, fn) => {
  let lastError;
  let attempts = 0;
  while (attempts < count) {
    attempts++;
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      console.log(`Attempt ${attempts} failed: ${error.message}`);
      if (attempts >= count) {
        break;
      }
      console.log(`Retrying in ${delay / 1000} seconds...`);
      if (delay) await sleep(delay);
    }
  }
  throw lastError;
};

const samTemplateFile = fs.readFileSync("template.yaml", "utf8");
const samTemplate = yamlParse(samTemplateFile);
const resources = samTemplate.Resources;
const dynamoDbResources = Object.values(resources)
  .filter((resource) => resource.Type === "AWS::DynamoDB::Table" || resource.Type === "AWS::Serverless::SimpleTable")
  .map((resource) => resource.Properties);

const url = process.env.DYNAMODB_ENDPOINT;
const region = process.env.AWS_REGION;
const accessKeyId = process.env.AWS_ACCESS_KEY_ID;
const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
const client = new DynamoDBClient({
  region,
  credentials: { accessKeyId, secretAccessKey },
  endpoint: { url: new URL(url) },
});

// List tables
const listTablesCommand = new ListTablesCommand({});
// Retry until the DynamoDB server is ready
console.log("Connecting to Dynamodb local server");
const listTablesResponse = await retry(10, 2000, () => client.send(listTablesCommand));
const currentTables = listTablesResponse.TableNames;

const tablesToDelete = currentTables.filter(
  (table) => !dynamoDbResources.some((resource) => resource.TableName === table),
);
const tablesToCreate = dynamoDbResources.filter((resource) => !currentTables.includes(resource.TableName));
const tablesToUpdate = dynamoDbResources.filter((resource) => currentTables.includes(resource.TableName));

const tempDir = "tables";

await Promise.all([
  // Delete tables
  ...tablesToDelete.map(async (tableName) => {
    const deleteTableCommand = new DeleteTableCommand({
      TableName: tableName,
    });
    await Promise.all([client.send(deleteTableCommand), fs.remove(path.resolve(tempDir, `${tableName}.json`))]);
    console.log(`Deleted table ${tableName}`);
  }),

  // Create tables
  ...tablesToCreate.map(async (table) => {
    const createTableCommand = new CreateTableCommand(table);
    const previousTableFile = path.resolve(tempDir, `${table.TableName}.json`);
    await Promise.all([client.send(createTableCommand), fs.writeJson(previousTableFile, table)]);
    console.log(`Created table ${table.TableName}`);
  }),

  // Update tables
  ...tablesToUpdate.map(async (table) => {
    // Check if the table properties have changed
    const previousTableFile = path.resolve(tempDir, `${table.TableName}.json`);
    const previousTableExists = fs.pathExistsSync(previousTableFile);
    if (previousTableExists) {
      const previousTable = await fs.readJson(previousTableFile);
      if (isEqual(table, previousTable)) {
        console.log(`Table ${table.TableName} has not changed`);
        return;
      }
    }
    const updateTableCommand = new UpdateTableCommand(table);
    await Promise.all([client.send(updateTableCommand), fs.writeJson(previousTableFile, table)]);
    console.log(`Updated table ${table.TableName}`);
  }),
]);

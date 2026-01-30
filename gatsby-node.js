const { algoliasearch } = require("algoliasearch");
const chunk = require("lodash.chunk");
const path = require("path");

/**
 * @typedef {import('gatsby').GatsbyCache} GatsbyCache
 */

/**
 * @typedef {import('gatsby').Reporter} Reporter
 */

/**
 * @typedef {import('algoliasearch').SearchIndex} SearchIndex
 */

/**
 * @typedef {import('gatsby').graphql} graphql
 */

/**
 * @typedef {import('@algolia/client-search').Settings} Settings
 */

/**
 * @typedef Query
 * @property {string} query The graphql query
 * @property {object=} queryVariables Allows you to use graphql query variables in the query
 * @property {Function} transformer transform the results of the query into objects. Likely `({ data }) => data.myProperty.nodes`
 * @property {string=} indexName index name for this query
 * @property {Settings} settings index settings for this query
 * @property {boolean=} mergeSettings defaults to false. Whether settings set in the index are overridden or persisted
 */

/**
 * Fetches all records for the current index from Algolia
 *
 * @param {import('algoliasearch').SearchClient} client
 * @param {string} indexName
 * @param {Reporter} reporter
 * @param {GatsbyCache} cache
 */
function fetchExistingObjects(client, indexName, reporter, cache) {
    const hits = {};

    return cache
        .get(`algolia-objects-${indexName}`)
        .then((values) => {
            if (!values || Object.keys(values).length === 0) {
                throw new Error("cache actually failed");
            }
            return values;
        })
        .catch(() =>
            client
                .browseObjects({
                    indexName: indexName,
                    batch: (batch) => {
                        if (Array.isArray(batch)) {
                            batch.forEach((hit) => {
                                if (hit.internal?.contentDigest) {
                                    hits[hit.objectID] = hit;
                                }
                            });
                        }
                    },
                    attributesToRetrieve: ["internal.contentDigest"],
                })
                .then(() => hits)
                .catch((err) => {
                    // If index doesn't exist (404), return empty hits - it will be created when we save objects
                    if (err.status === 404) {
                        return hits;
                    }
                    reporter.panicOnBuild("failed while getting indexed objects", err);
                })
        );
}

/**
 * @typedef PluginConfiguration
 * @property {string} appId
 * @property {string} apiKey
 * @property {import('algoliasearch').AlgoliaSearchOptions} algoliasearchOptions
 * @property {Query[]} queries
 * @property {string} indexName
 * @property {boolean} concurrentQueries
 * @property {boolean} dryRun
 * @property {boolean} continueOnFailure
 */

exports.onPostBuild = async (
    /** @type {{graphql: graphql, reporter: Reporter, cache: GatsbyCache}} */ { graphql, reporter, cache },
    /** @type {PluginConfiguration} */ config
) => {
    const {
        appId,
        apiKey,
        queries,
        concurrentQueries = true,
        dryRun = false,
        continueOnFailure = false,
    } = config;

    console.log("Queries", queries);

    const activity = reporter.activityTimer("index to Algolia");
    activity.start();

    if (dryRun === true) {
        console.log(
            "\x1b[33m%s\x1b[0m",
            "==== THIS IS A DRY RUN ====================\n" +
            "- No records will be pushed to your index\n" +
            "- No settings will be updated on your index"
        );
    }

    if (continueOnFailure === true && !(appId && apiKey)) {
        activity.setStatus("options.continueOnFailure is true and api key or appId are missing; skipping indexing");
        activity.end();
        return;
    }

    const client = algoliasearch(appId, apiKey);

    activity.setStatus(`${queries.length} queries to index`);

    try {
        // combine queries with the same index to prevent overwriting data
        const groupedQueries = groupQueriesByIndex(queries, config);

        const jobs = [];
        for (const [indexName, indexQueries] of Object.entries(groupedQueries)) {
            const queryPromise = runIndexQueries(indexName, indexQueries, {
                client,
                activity,
                graphql,
                cache,
                config,
                reporter,
                dryRun,
            });

            if (concurrentQueries) {
                jobs.push(queryPromise);
            } else {
                // await each individual query rather than batching them
                const res = await queryPromise;
                jobs.push(res);
            }
        }

        await Promise.all(jobs);
    } catch (err) {
        if (continueOnFailure) {
            reporter.warn("failed to index to Algolia");
            console.error(err);
        } else {
            activity.panicOnBuild("failed to index to Algolia", err);
        }
    }

    activity.end();
};

/**
 * @param {PluginConfiguration} config
 */
function groupQueriesByIndex(queries = [], config) {
    const { indexName: mainIndexName } = config;

    return queries.reduce((groupedQueries, queryOptions) => {
        const { indexName = mainIndexName } = queryOptions;

        return {
            ...groupedQueries,
            [indexName]: [...(Object.hasOwn(groupedQueries, indexName) ? groupedQueries[indexName] : []), queryOptions],
        };
    }, {});
}

/**
 * Run all queries for a given index, then make any updates / removals necessary
 * @param {string} indexName
 * @param {object[]} queries
 * @param {object} options
 * @param {import('algoliasearch').SearchClient} options.client
 * @param {import('gatsby').ActivityTracker} options.activity
 * @param {graphql} options.graphql
 * @param {Reporter} options.reporter
 * @param {PluginConfiguration} options.config
 * @param {GatsbyCache} options.cache
 * @param {boolean=} options.dryRun
 */
async function runIndexQueries(
    indexName,
    queries = [],
    { client, activity, graphql, reporter, cache, config, dryRun }
) {
    const { settings: mainSettings, chunkSize = 1000 } = config;

    activity.setStatus("Getting existing objects");

    const existingClient = await clientExists(client, indexName);

    // get all indexed objects matching all matched fields
    const indexedObjects = await fetchExistingObjects(existingClient, indexName, reporter, cache);

    activity.setStatus(
        `Running ${queries.length} ${queries.length === 1 ? "query" : "queries"} for index ${indexName}...`
    );

    const objectMapsByQuery = await Promise.all(queries.map((query) => getObjectsMapByQuery(query, graphql, reporter)));

    const allObjectsMap = objectMapsByQuery.reduce((acc, objectsMap = {}) => {
        return Object.assign(acc, objectsMap);
    }, {});

    cache.set(
        `algolia-objects-${indexName}`,
        Object.fromEntries(
            Object.entries(allObjectsMap).map(([objectID, object]) => [
                objectID,
                { internal: { contentDigest: object.internal?.contentDigest } },
            ])
        )
    );

    activity.setStatus(
        `${queries.length === 1 ? "Query" : "Queries"} resulted in a total of ${Object.keys(allObjectsMap).length
        } results`
    );

    let toIndex = {}; // used to track objects that should be added / updated
    const toRemove = {}; // used to track objects that are stale and should be removed

    // iterate over each query to determine which data are fresh
    activity.setStatus("Starting Partial updates...");

    // iterate over each query
    for (const [i] of queries.entries()) {
        const queryResultsMap = objectMapsByQuery[i] || {};

        // iterate over existing objects and compare to fresh data
        for (const [id, existingObj] of Object.entries(indexedObjects)) {
            if (Object.hasOwn(queryResultsMap, id)) {
                // key matches fresh objects, so compare match fields
                const newObj = queryResultsMap[id];
                if (!newObj.internal?.contentDigest) {
                    reporter.panicOnBuild(
                        "the objects must have internal.contentDigest. Current object:\n" +
                        JSON.stringify(newObj, null, 2)
                    );
                }

                if (existingObj.internal?.contentDigest !== newObj.internal.contentDigest) {
                    // contentDigest differs, so index new object
                    toIndex[id] = newObj;
                }
                // objects are the same, so skip

                // remove from queryResultsMap, since it is already accounted for
                delete queryResultsMap[id];
            } else {
                // remove existing object if it is managed and not returned from a query
                if (
                    // not in any query
                    !Object.hasOwn(allObjectsMap, id) &&
                    // managed by this plugin
                    existingObj.internal?.contentDigest
                ) {
                    toRemove[id] = true;
                }
            }
        }

        if (Object.values(queryResultsMap).length) {
            // stale objects have been removed, remaining query objects should be indexed
            toIndex = {
                ...toIndex,
                ...queryResultsMap,
            };
        }
    }

    const objectsToIndex = Object.values(toIndex);
    const objectsToRemove = Object.keys(toRemove);

    // Check if index exists before we start making changes
    const indexAlreadyExists = await indexExists(client, indexName);

    if (objectsToIndex.length) {
        const chunks = chunk(objectsToIndex, chunkSize);

        activity.setStatus(`Found ${objectsToIndex.length} new / updated records...`);

        if (chunks.length > 1) {
            activity.setStatus(`Splitting in ${chunks.length} jobs`);
        }

        /* Add changed / new objects */
        const chunkJobs = chunks.map(async (chunked) => {
            if (dryRun === true) {
                reporter.info(`[dry run]: ${objectsToIndex.length} records to add`);
            } else {
                await client.saveObjects({
                    indexName: indexName,
                    objects: chunked
                });
            }
        });

        await Promise.all(chunkJobs);
    } else {
        activity.setStatus("No updates necessary; skipping!");
    }

    if (objectsToRemove.length) {
        activity.setStatus(`Found ${objectsToRemove.length} stale objects; removing...`);

        if (dryRun === true) {
            reporter.info(`[dry run]: ${objectsToRemove.length} records to delete`);
        } else {
            const response = await client.deleteObjects({
                indexName: indexName,
                objectIDs: objectsToRemove,
            });
            // Wait for task to finish (optional)
            await client.waitForTask({ indexName: indexName, taskID: response.taskID });
        }
    }

    // defer to first query for index settings
    // todo: maybe iterate over all settings and throw if they differ
    const { settings = mainSettings, mergeSettings = false, forwardToReplicas } = queries[0] || {};

    // Only apply settings if:
    // 1. We have settings to apply, AND
    // 2. Either the index already existed OR we just created it by saving objects
    if (settings && (indexAlreadyExists || objectsToIndex.length > 0)) {
        const settingsToApply = await getSettingsToApply({
            settings,
            mergeSettings,
            client,
            indexName,
            reporter,
        });

        if (dryRun) {
            console.log("[dry run]: settings", settingsToApply);
        } else {
            const { taskID } = await client.setSettings({
                indexName: indexName,
                indexSettings: settingsToApply,
                forwardToReplicas,
            });
            await client.waitForTask({ indexName: indexName, taskID });
        }
    }

    activity.setStatus("Done!");
}

/**
 * Does an Algolia index exist already
 * @param {import('algoliasearch').SearchClient} client
 */
function indexExists(client, indexName) {
    return client
        .getSettings({ indexName: indexName })
        .then(() => true)
        .catch((error) => {
            if (error.status !== 404) {
                throw error;
            }

            return false;
        });
}

/**
 * @param {object} options
 * @param {Settings} options.settings
 * @param {boolean} options.mergeSettings
 * @param {import('algoliasearch').SearchClient} options.client
 * @param {string} options.indexName
 * @param {Reporter} options.reporter
 * @returns {Promise<Settings>}
 */
async function getSettingsToApply({ settings, mergeSettings, client, indexName, reporter }) {
    const existingSettings = await client.getSettings({ indexName: indexName }).catch((e) => {
        // If index doesn't exist (404), return empty settings - it will be created when we save objects
        if (e.status === 404) {
            return {};
        }
        reporter.panicOnBuild(`${e.toString()} ${indexName}`);
    });

    if (!settings) {
        return existingSettings;
    }

    return {
        ...(mergeSettings ? existingSettings : {}),
        ...settings,
    };
}

/**
 * @param {Query} options
 * @param {graphql} graphql
 * @param {Reporter} reporter
 */
async function getObjectsMapByQuery({ query, queryVariables, transformer = (x) => x }, graphql, reporter) {
    const result = await graphql(query, queryVariables);
    if (result.errors) {
        reporter.panicOnBuild(`failed to index to Algolia, errors:\n ${JSON.stringify(result.errors)}`, result.errors);
    }

    const objects = (await transformer(result)).map((object) => ({
        objectID: object.id,
        ...object,
    }));

    if (objects.length > 0 && !objects[0].objectID) {
        reporter.panicOnBuild(`failed to index to Algolia. Query does not have 'id'`);
    }

    // return a map by id for later use
    return Object.fromEntries(objects.map((object) => [object.objectID, object]));
}

/**
 * @param {algoliasearch.SearchClient} client
 * @param {string} indexName
 */
async function clientExists(client, indexName) {
    // Check if index exists, but don't create it if it doesn't
    // In Algolia v5, indexes are created automatically when objects are first saved
    const exists = await indexExists(client, indexName);

    if (!exists) {
        // Index will be created automatically when we save objects
        // No need to explicitly create it with setSettings
    }

    return client;
}

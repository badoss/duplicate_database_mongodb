const { MongoClient } = require("mongodb");
require('dotenv').config()
async function duplicateAllCollections() {
    const sourceUrl = process.env.MONGO_SOURCE_URL;
    const destUrl = process.env.MONGO_DEST_URL;

    const sourceClient = new MongoClient(sourceUrl);
    const destClient = new MongoClient(destUrl);
    
    try {
        await sourceClient.connect();
        await destClient.connect();
        const sourceDB = sourceClient.db(process.env.SOURCE_DB);
        const sourceCollectionNames = await sourceDB.listCollections().toArray();
        // console.log(sourceCollectionNames)
        // dump collection
        for (const collectionInfo of sourceCollectionNames) {
            const sourceCollectionName = collectionInfo.name;
            if (collectionInfo.name.split('.')[0] === "system") {
                continue
            } else {
                if (collectionInfo.type === "collection") {
                    const destCollectionName = sourceCollectionName;
                    const sourceCollection = sourceDB.collection(sourceCollectionName);
                    const destDB = destClient.db(process.env.DEST_DB);
                    const destCollection = destDB.collection(destCollectionName);
                    const documentsToDuplicate = await sourceCollection.find({}).toArray();
                    if (documentsToDuplicate.length !== 0) {
                        // console.log(documentsToDuplicate.length)
                        console.log('create collection : ' ,collectionInfo.name , ' count : ' , documentsToDuplicate.length)
                        await destCollection.insertMany(documentsToDuplicate);
                    }
                }
            }
        }
        // dump view
        for (const collectionInfo of sourceCollectionNames) {
            const sourceCollectionName = collectionInfo.name;
            if (collectionInfo.type === 'view') {
                console.log('create view : ' ,collectionInfo.name )
                const destDB = destClient.db(process.env.DEST_DB);
                await destDB.createCollection(sourceCollectionName, collectionInfo.options);
            }

        }
        // dump timeserise
        for (const collectionInfo of sourceCollectionNames) {
            const sourceCollectionName = collectionInfo.name;
            if (collectionInfo.type === 'timeseries') {
                const sourceCollection = sourceDB.collection(sourceCollectionName);
                const documentsToDuplicate = await sourceCollection.find({}).toArray();
                // console.log(documentsToDuplicate)
                const destDB = destClient.db(process.env.DEST_DB);
                await destDB.createCollection(sourceCollectionName, collectionInfo.options)
                console.log('create timeserise : ' ,collectionInfo.name , ' count : ' , documentsToDuplicate.length)
                if (documentsToDuplicate.length !== 0) {
                    const destCollection = destDB.collection(sourceCollectionName);
                    await destCollection.insertMany(documentsToDuplicate);
                    // console.log('add log timeserise ' ,collectionInfo.name , ' success')
                }
            }
        }
        console.log("All collections duplicated successfully");
    } catch (err) {
        console.error("Error duplicating collections:", err);
    } finally {
        // Close the source and destination database connections
        await sourceClient.close();
        await destClient.close();
    }
}
duplicateAllCollections();

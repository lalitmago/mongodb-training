package com.lama.mongodb.training;

import com.mongodb.*;
import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.internal.connection.CommandHelper;
import org.bson.Document;

public class Practice {

    private static MongoDatabase getDatabase(String databaseName) {
        MongoClient mongoClient = new MongoClient("localhost", 27017);

        MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);


        return mongoDatabase;
    }

    public static void getMostComments(MongoDatabase mongoDatabase) {
        FindIterable<Document> result = mongoDatabase
                .getCollection("stories")
                .find()
                .projection(Projections.include("title", "comments"))
                .sort(Sorts.descending("comments"))
                .limit(1);

        for(Document doc : result) {
            System.out.println(doc.get("title") + " : " + doc.get("comments"));
        }
    }

    public static void getStudents(MongoDatabase mongoDatabase) {

        long totalCount = mongoDatabase.getCollection("grades").countDocuments();
        long filteredCount = mongoDatabase.getCollection("grades").countDocuments(Filters.lt("student_id", 65));

        System.out.println("Total Count : " + totalCount + ", Documents meeting the criteria : " + filteredCount);
    }

    public static void explain(MongoDatabase mongoDatabase, String collection) {

        FindIterable<Document> document = mongoDatabase.getCollection("messages").find();

        //work on this next time
        mongoDatabase.runCommand(new Document()
                .append("explain", null));

    }

    private static void createIndex(MongoDatabase mongoDatabase, String collection, String indexName) {

        IndexOptions indexOptions = new IndexOptions().name(indexName);
        String createdIndex = mongoDatabase.getCollection(collection)
                .createIndex(Indexes.ascending("timestamp"), indexOptions);

        System.out.println("Created index : " + createdIndex);
    }

    private static void dropIndex(MongoDatabase mongoDatabase, String collection, String indexName) {

        System.out.println("Dropping index " + indexName);
        mongoDatabase.getCollection(collection)
                .dropIndex(indexName);

    }

    private static void listIndexes(MongoDatabase mongoDatabase, String collection) {
        MongoCollection mongoCollection = mongoDatabase.getCollection(collection);

        ListIndexesIterable listIndexesIterable = mongoCollection.listIndexes();

        for(Object index : listIndexesIterable )
            System.out.println(index);
    }

    public static void main(String[] args) {

        MongoDatabase mongoDatabase = getDatabase("sample");

        //ClientSession clientSession = new


        /*System.out.println("First one...!");
        getMostComments(mongoDatabase);

        System.out.println("Second one...!");
        getStudents(mongoDatabase);

        System.out.println("Third one...!");

        explain(mongoDatabase);*/

        createIndex(mongoDatabase, "messages", "myIndex");
        listIndexes(mongoDatabase, "messages");
        dropIndex(mongoDatabase, "messages", "myIndex");
        listIndexes(mongoDatabase, "messages");

        createIndex(mongoDatabase, "blog", "blogComments");



    }
}

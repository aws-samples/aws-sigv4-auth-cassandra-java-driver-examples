package software.aws.mcs.example;

/*-
 * #%L
 * AWS SigV4 Auth Java Driver 4.x Examples
 * %%
 * Copyright (C) 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * %%
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * #L%
 */

import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import software.aws.mcs.auth.SigV4AuthProvider;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class OrderFetcher {
    private final static String HOST_NAME_CASSANDRA="cassandra.sa-east-1.amazonaws.com";
    private final static int CASSANDRA_PORT=9142;
    private final static Regions clientRegion = Regions.SA_EAST_1;
    private final static String bucketName = "keyspace-bucket-dev";
    private final static String objectName = "cassandra-data-example.csv";

    public static void main(String [] args) throws SdkClientException, IOException {
        putObjectS3();
        cassandraTest();
    }

    private static void cassandraTest() throws IOException {
        SigV4AuthProvider provider = new SigV4AuthProvider(Regions.SA_EAST_1.getName());
        List<InetSocketAddress> contactPoints = Collections.singletonList(new InetSocketAddress(HOST_NAME_CASSANDRA, CASSANDRA_PORT));

        try (CqlSession session = CqlSession.builder().addContactPoints(contactPoints).withAuthProvider(provider).withLocalDatacenter(Regions.SA_EAST_1.getName()).build()) {
            insertCassandra(session);
            readCassandra(session);
        }
    }

    private static void readCassandra(CqlSession session) {
        PreparedStatement prepared = session.prepare("select * from bookstore.books");
        System.out.println("Executing query bookstore.books...");
        ResultSet rs = session.execute(prepared.bind());

        for (Row row : rs) {
            System.out.println(" ISBP= "+row.getString("isbn")+
                    " AUTHOR= "+row.getString("author")+
                    " PAGES= "+row.getInt("pages")+
                    " TITLE= "+row.getString("title")+
                    " YEAR_OF_PUBLICATON= "+row.getInt("year_of_publication"));
            System.out.println();
        }
        System.out.println();
    }

    private static void insertCassandra(CqlSession session) throws IOException{
        List<Book> books = getObjectContentS3();
        System.out.println("Execution insert bookstore.books...");
        PreparedStatement insertBook = session.prepare("insert into bookstore.books"
                +"(isbn, title, author, pages, year_of_publication)"+ "values(?, ?, ?, ?, ?)");
        books.stream().forEach((Book book)->{
            BoundStatement boundStatement = insertBook.bind()
                    .setString(0,book.getIsbn())
                    .setString(1,book.getTitle())
                    .setString(2,book.getAuthor())
                    .setInt(3,book.getPages())
                    .setInt(4,book.getYear())
                    .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
            ResultSet resultSet = session.execute(boundStatement);
        });
        System.out.println("done");
    }

    private static List<Book> getObjectContentS3() throws IOException {
        AmazonS3 s3Client = getS3Client(clientRegion);
        System.out.println("Downloading an object");
        S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucketName, objectName));
        System.out.println("Content-Type "+ fullObject.getObjectMetadata().getContentType());
        System.out.println("Content: ");
        System.out.println();
        List<Book> books = convertToBooks(fullObject.getObjectContent());
        return books;
    }

    private static void putObjectS3() throws IOException {
        AmazonS3 s3Client = getS3Client(clientRegion);
        System.out.format("Uploading %s o S3 bucket %s...\n", objectName, bucketName);
        PutObjectRequest putRequest = new PutObjectRequest(bucketName,
                objectName, new File(objectName)).withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams());
        s3Client.putObject(putRequest);
        System.out.println("done");
    }

    private static void deleteObjectS3() throws IOException {
        AmazonS3 s3Client = getS3Client(clientRegion);
        System.out.format("Deleting object %s from S3 bucket: %s\n", objectName, bucketName);
        s3Client.deleteObject(bucketName, objectName);
        System.out.println("done");
    }

    private static List<Book> convertToBooks(InputStream input) throws IOException {
        List<Book> books = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;
        while((line = reader.readLine()) != null){
            System.out.println(line);
            String[] item = line.split(";");
            Book book = new Book();
            book.setIsbn(item[0]);
            book.setTitle(item[1]);
            book.setAuthor(item[2]);
            book.setPages(Integer.parseInt(item[3]));
            book.setYear(Integer.parseInt(item[4]));
            books.add(book);
        }
        return books;
    }

    private static void listBuckets() throws IOException {
        final AmazonS3 s3 = getS3Client(clientRegion);
        List<Bucket> buckets = s3.listBuckets();
        System.out.println("Os bucketsS3 sao: ");
        for (Bucket b : buckets) {
            System.out.println("* " + b.getName());
        }
    }

    private static AmazonS3 getS3Client(Regions clientRegion) {
        return AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .build();
    }
}

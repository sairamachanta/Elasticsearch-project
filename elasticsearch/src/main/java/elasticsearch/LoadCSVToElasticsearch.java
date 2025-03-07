package elasticsearch;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVReader;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

public class LoadCSVToElasticsearch {
	
	
	public static void main(String[] args) throws Exception {
		
		
		loadCSVToElasticsearch();
		
		String indexName = "employees"; // Elasticsearch index name
		String csvFilePath = "F:\\Employee Sample Data.csv"; // Path to CSV file
		  BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		  credentialsProvider.setCredentials(AuthScope.ANY, 
              new UsernamePasswordCredentials("elastic", "=myiz2VHmutSgu4z=ZPO"));

          // Build Elasticsearch client with authentication
          RestClient restClient = RestClient.builder(new HttpHost("127.0.0.1", 9200, "http"))
              .setHttpClientConfigCallback(httpClientBuilder -> 
                  httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
              .build();

		// Create Elasticsearch Client
		//RestClient restClient = RestClient.builder(new HttpHost("127.0.0.1", 9200, "http")).build();
		RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
		ElasticsearchClient esClient = new ElasticsearchClient(transport);

		try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
			List<String[]> records = reader.readAll();
			List<BulkOperation> bulkOperations = new ArrayList<>();
			// Assuming CSV Header: id, name, age, department
			for (int i = 1; i < records.size(); i++) { // Skip header
				String[] row = records.get(i);

				// Convert CSV row to JSON-like Map
//				Map<String, Object> employee = Map.of("id", row[0], "name", row[1], "phone", row[2], "age",
//						Integer.parseInt(row[3]), "department", row[4]);
				
				
				Map<String, Object> employee = new HashMap<>();
				employee.put("EMPLOYEE_ID", row[0]);
				employee.put("FIRST_NAME", row[1]);
				employee.put("LAST_NAME", row[2]);
				employee.put("EMAIL", row[3]);
				employee.put("PHONE_NUMBER", row[4]);
				
				if(row[0]!=null&&!row[0].isEmpty()&&row[1]!=null&&!row[1].isEmpty()&&row[2]!=null&&!row[2].isEmpty()&&row[3]!=null&&!row[3].isEmpty()&&row[4]!=null&&!row[4].isEmpty()) {

				// Create a BulkOperation for indexing
				bulkOperations.add(BulkOperation.of(
						b -> b.index(IndexOperation.of(iop -> iop.index(indexName).id(row[0]).document(employee)))));
				
				// Create an IndexRequest to index the data
				IndexRequest<Map<String, Object>> indexRequest = new IndexRequest.Builder<Map<String, Object>>()
						.index(indexName).id(row[0]).document(employee) // The document to index
						.build(); 
				IndexResponse indexResponse = esClient.index(indexRequest);
				if (indexResponse.result() != null) {
					System.out.println("Employee data indexed successfully with ID: " + indexResponse.id());
				} else {
					System.out.println("Failed to index employee data.");
				}
			}
			}
			// Perform Bulk Indexing
			BulkResponse response = esClient.bulk(b -> b.index(indexName).operations(bulkOperations));
			System.out.println("Indexed " + response.items().size() + " employees from CSV");

            System.out.println("searching for  employeeId =200 ");
			String employeeId = "200";
			searchEmployeeData(esClient, indexName, employeeId);


            System.out.println(" Searching for total number of employess ");
			countEmployees(esClient, indexName);
			// Create a CountRequest to get the total document count


            System.out.println("delecting the employee id =100");
			deleteEmployeeById(esClient, indexName, "100");
			// Create a DeleteRequest to delete the document

            System.out.println("finding employees in department");
			String departmentName = "100";
			searchEmployeesByDepartment(esClient, indexName, departmentName);
		

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				transport.close();
				restClient.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static void searchEmployeesByDepartment(ElasticsearchClient esClient, String indexName,
			String departmentName) {
		try {
			SearchRequest searchRequest = new SearchRequest.Builder().index(indexName) // The index name
					.query(q -> q.term(t -> t.field("department").value(departmentName))).build();
	
			SearchResponse<Object> searchResponse = esClient.search(searchRequest, Object.class);
			if (searchResponse.hits().total().value() > 0) {
				System.out.println("Found " + searchResponse.hits().total().value() + " employees in the "
						+ departmentName + " department.");
				searchResponse.hits().hits().forEach(hit -> {
					System.out.println("Employee: " + hit.source());
				});
			} else {
				System.out.println("No employees found in the " + departmentName + " department.");
			}
		} catch(Exception e) {
            e.printStackTrace();
		}
		
	}

	private static void deleteEmployeeById(ElasticsearchClient esClient, String indexName, String employeeId) {
		try {
			DeleteRequest deleteRequest = new DeleteRequest.Builder().index(indexName) // The index name
					.id(employeeId) // The document ID to be deleted
					.build();
			DeleteResponse deleteResponse = esClient.delete(deleteRequest);
			if (deleteResponse.result() != null) {
				System.out.println("Employee with ID " + employeeId + " deleted successfully.");
			} else {
				System.out.println("Employee with ID " + employeeId + " not found.");
			}
		} catch (Exception e) {
            e.printStackTrace();
		}
		
	}

	private static void countEmployees(ElasticsearchClient esClient, String indexName) {
		try {
			CountRequest countRequest = new CountRequest.Builder().index(indexName) // The index name
					.build();
			CountResponse countResponse = esClient.count(countRequest);
			System.out.println("Total number of employees: " + countResponse.count());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void searchEmployeeData(ElasticsearchClient esClient, String indexName, String employeeId) {
		try {
			GetRequest getRequest = new GetRequest.Builder().index(indexName) // The index name
					.id(employeeId)
					.build();
			GetResponse<Object> getResponse = esClient.get(getRequest, Object.class);
			if (getResponse.found()) {
				System.out.println("Employee Details: " + getResponse.source());
			} else {
				System.out.println("Employee with ID " + employeeId + " not found.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void loadCSVToElasticsearch() {
		
		
	}

}
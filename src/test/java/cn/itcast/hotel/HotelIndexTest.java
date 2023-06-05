package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.boot.test.context.SpringBootTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelIndexTest {
    private RestHighLevelClient client;

    @org.junit.jupiter.api.Test
    void testInit() {
        System.out.println(client);
    }

    @BeforeEach
    void beforeAll() {
        this.client= new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.88.130:9200")
        ));
    }

    @AfterEach
    void afterAll() throws IOException {
        this.client.close();
    }

    @Test
    void createHotelIndex() throws IOException {
        // 1. 创建Request对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");

        // 2. 准备请求的参数: DSL语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);

        // 3. 发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteHotelIndex() throws IOException {
        // 1.创建Request对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        // 2.准备请求的参数: DSL语句
//        request.indices();
        // 2.发送请求
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    @Test
    void name() throws IOException {
        // 1.创建Request对象
        GetIndexRequest request = new GetIndexRequest("hotel");
        // 2.发起请求
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        // 3.输出
        if (exists) System.out.println("存在");
        else System.out.println("不存在");
    }



}

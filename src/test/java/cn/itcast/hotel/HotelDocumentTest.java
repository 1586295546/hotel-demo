package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelDocumentTest {
    private RestHighLevelClient client;

    @Autowired
    private IHotelService hotelService;


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
    void testAddDocument() throws IOException {
        // 根据id到数据库查询酒店数据
        Hotel hotel = hotelService.getById(61083L);
        // 转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);

        // 1.准备Request对象
        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
        // 2.准备Json文档
        request.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
        // 3.发送请求
        client.index(request, RequestOptions.DEFAULT);
    }

    @Test
    void testGetDocumentById() throws IOException {
        // 1.创建request对象
        GetRequest request = new GetRequest("hotel", "61083");
        // 2.发送请求, 得到结果
        GetResponse documentFields = client.get(request, RequestOptions.DEFAULT);
        // 3.解析结果
        String sourceAsString = documentFields.getSourceAsString();
        // 4.打印结果
        HotelDoc hotelDoc = JSON.parseObject(sourceAsString, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    @Test
    void testUpdateDocument() throws IOException {
        // 1.准备Requst
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        // 2.准备请求参数
        request.doc(
                "price", 925,
                "starName", "六钻"
        );
        // 3.发送请求
        client.update(request, RequestOptions.DEFAULT);

    }

    @Test
    void testDeleteDocument() throws IOException {
        // 1.准备Request
        DeleteRequest request = new DeleteRequest("hotel","61083");
        // 2.发送请求
        client.delete(request, RequestOptions.DEFAULT);

    }

    /**
     * 批量导入文档
     * @throws IOException
     */
    @Test
    void testBulk() throws IOException {
        // 批量查询酒店数据
        List<Hotel> hotels = hotelService.list();
        // 转换为文档类型HotelDoc
        List<HotelDoc> collect = hotels.stream().map(HotelDoc::new).collect(Collectors.toList());

        // 1.创建Bulk请求
        BulkRequest request = new BulkRequest();
        // 2.添加要批量提交的请求: 这里添加了两个新增文档的请求
        collect.forEach(i->request.add(
                new IndexRequest("hotel").id(i.getId().toString()).source(JSON.toJSONString(i)
                        , XContentType.JSON)));
        // 3.发送请求
        client.bulk(request, RequestOptions.DEFAULT);
    }

    /**
     * 批量查询文档:单个请求查询方法
     */
    @Test
    void testGetAll() {
        List<Hotel> list = hotelService.list();
        list.forEach(i -> {
            //1. 创建request对象
            GetRequest request1 = new GetRequest("hotel", i.getId().toString());
            //2. 发送请求,得到结果
            GetResponse documentFields = null;
            try {
                documentFields = client.get(request1, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            //3. 解析结果并打印
            String sourceAsString = documentFields.getSourceAsString();
            System.out.println(JSON.parseObject(sourceAsString, HotelDoc.class));
        });
    }

    @Test
    void testGetAll2() {
        List<Hotel> list = hotelService.list();
        // 1. 创建MultiGetRequest对象
        MultiGetRequest request = new MultiGetRequest();
        // 2.批量提交请求
        list.forEach(i -> request.add(new MultiGetRequest.Item("hotel",i.getId().toString())));
        // 3.发送请求, 得到结果
        try {
            MultiGetResponse multiGetItemResponses = client.mget(request, RequestOptions.DEFAULT);
            // 4.解析结果
            for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
                GetResponse response = itemResponse.getResponse();
                if (response.isExists()) {
                    String json = response.getSourceAsString();
                    // 打印结果
                    System.out.println(json);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}

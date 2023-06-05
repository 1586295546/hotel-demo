package cn.itcast.hotel;

import org.springframework.boot.test.context.SpringBootTest;

import java.util.Random;

@SpringBootTest
public class Test {

    @org.junit.jupiter.api.Test
    void test1() {
        for (int i = 0; i < 100; i++) {
            Random random = new Random();
            int codePoint = 0x4E00 + random.nextInt(0x9FA5 - 0x4E00);
            char c = (char) codePoint;
            System.out.println(c);
        }
    }
}

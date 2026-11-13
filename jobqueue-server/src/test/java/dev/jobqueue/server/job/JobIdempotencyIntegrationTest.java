package dev.jobqueue.server.job;

import com.jayway.jsonpath.JsonPath;
import dev.jobqueue.server.PostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "jobqueue.reaper.enabled=false")
@AutoConfigureMockMvc
class JobIdempotencyIntegrationTest extends PostgresIntegrationTest {

    private static final int PARALLEL_REQUESTS = 100;

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    MockMvc mvc;

    @Test
    void shouldCreateOneJobAndReturnItToEveryoneWhenSameKeyIsSentInParallel() throws Exception {
        String body = "{\"queue\":\"idem-parallel\",\"type\":\"t\",\"idempotencyKey\":\"same\"}";
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(PARALLEL_REQUESTS);
        List<Future<int[]>> futures = new ArrayList<>();
        for (int i = 0; i < PARALLEL_REQUESTS; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                var response = mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                        .andReturn().getResponse();
                String id = JsonPath.read(response.getContentAsString(), "$.id");
                return new int[]{response.getStatus(), id.hashCode()};
            }));
        }
        start.countDown(); // release all threads together to maximise contention

        int created = 0;
        int ok = 0;
        Set<Integer> distinctIds = new HashSet<>();
        for (Future<int[]> future : futures) {
            int[] result = future.get();
            if (result[0] == 201) {
                created++;
            } else if (result[0] == 200) {
                ok++;
            }
            distinctIds.add(result[1]);
        }
        pool.shutdown();

        assertThat(created).isEqualTo(1);
        assertThat(ok).isEqualTo(PARALLEL_REQUESTS - 1);
        assertThat(distinctIds).hasSize(1);
        Integer rows = JDBC.sql("SELECT count(*) FROM jobs WHERE queue = 'idem-parallel' AND idempotency_key = 'same'")
                .query(Integer.class).single();
        assertThat(rows).isEqualTo(1);
    }

    @Test
    void shouldReturnOkWithSameJobWhenRequestIsRepeated() throws Exception {
        String body = "{\"queue\":\"idem-repeat\",\"type\":\"t\",\"idempotencyKey\":\"k\"}";

        String firstId = JsonPath.read(
                mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                        .andExpect(status().isCreated())
                        .andReturn().getResponse().getContentAsString(), "$.id");

        mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(firstId));
    }

    @Test
    void shouldCreateSeparateJobsWhenNoKeyIsGiven() throws Exception {
        String body = "{\"queue\":\"idem-none\",\"type\":\"t\"}";

        mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated());
        mvc.perform(post("/jobs").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated());

        Integer rows = JDBC.sql("SELECT count(*) FROM jobs WHERE queue = 'idem-none'")
                .query(Integer.class).single();
        assertThat(rows).isEqualTo(2);
    }
}

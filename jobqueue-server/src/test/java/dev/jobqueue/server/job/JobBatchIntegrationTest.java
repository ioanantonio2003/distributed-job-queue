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

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = "jobqueue.reaper.enabled=false")
@AutoConfigureMockMvc
class JobBatchIntegrationTest extends PostgresIntegrationTest {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    MockMvc mvc;

    @Test
    void shouldCreateAllJobsAndReturnIdsInOrderWhenBatchIsValid() throws Exception {
        String body = """
                {"jobs":[
                  {"queue":"batch-ok","type":"first","payload":{"n":1}},
                  {"queue":"batch-ok","type":"second","priority":5}
                ]}
                """;

        String response = mvc.perform(post("/jobs/batch").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.ids.length()").value(2))
                .andReturn().getResponse().getContentAsString();

        String firstId = JsonPath.read(response, "$.ids[0]");
        String secondId = JsonPath.read(response, "$.ids[1]");
        mvc.perform(get("/jobs/{id}", firstId))
                .andExpect(jsonPath("$.type").value("first"))
                .andExpect(jsonPath("$.payload.n").value(1))
                .andExpect(jsonPath("$.status").value("QUEUED"));
        mvc.perform(get("/jobs/{id}", secondId))
                .andExpect(jsonPath("$.type").value("second"))
                .andExpect(jsonPath("$.priority").value(5));
    }

    @Test
    void shouldInsertThousandJobsWhenBatchIsAtTheLimit() throws Exception {
        String jobs = IntStream.range(0, 1000)
                .mapToObj(i -> "{\"queue\":\"batch-1000\",\"type\":\"t\",\"payload\":{\"i\":" + i + "}}")
                .collect(Collectors.joining(","));

        mvc.perform(post("/jobs/batch").contentType(MediaType.APPLICATION_JSON).content("{\"jobs\":[" + jobs + "]}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.ids.length()").value(1000));

        assertThat(countJobsIn("batch-1000")).isEqualTo(1000);
    }

    @Test
    void shouldStoreNothingWhenOneJobInTheBatchIsInvalid() throws Exception {
        String body = """
                {"jobs":[
                  {"queue":"batch-invalid","type":"ok"},
                  {"queue":"","type":"broken"}
                ]}
                """;

        mvc.perform(post("/jobs/batch").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors['jobs[1].queue']").exists());

        assertThat(countJobsIn("batch-invalid")).isZero();
    }

    @Test
    void shouldStoreNothingWhenAPayloadInTheBatchIsNotAnObject() throws Exception {
        String body = """
                {"jobs":[
                  {"queue":"batch-payload","type":"ok"},
                  {"queue":"batch-payload","type":"bad","payload":"text"}
                ]}
                """;

        mvc.perform(post("/jobs/batch").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());

        assertThat(countJobsIn("batch-payload")).isZero();
    }

    @Test
    void shouldRejectBatchWhenAJobHasIdempotencyKey() throws Exception {
        String body = "{\"jobs\":[{\"queue\":\"batch-key\",\"type\":\"t\",\"idempotencyKey\":\"k\"}]}";

        mvc.perform(post("/jobs/batch").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("idempotencyKey is not supported in batch requests"));

        assertThat(countJobsIn("batch-key")).isZero();
    }

    @Test
    void shouldRejectBatchWhenListIsEmpty() throws Exception {
        mvc.perform(post("/jobs/batch").contentType(MediaType.APPLICATION_JSON).content("{\"jobs\":[]}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void shouldRejectBatchWhenMoreThanThousandJobs() throws Exception {
        String jobs = IntStream.range(0, 1001)
                .mapToObj(i -> "{\"queue\":\"batch-too-big\",\"type\":\"t\"}")
                .collect(Collectors.joining(","));

        mvc.perform(post("/jobs/batch").contentType(MediaType.APPLICATION_JSON).content("{\"jobs\":[" + jobs + "]}"))
                .andExpect(status().isBadRequest());

        assertThat(countJobsIn("batch-too-big")).isZero();
    }

    private static Integer countJobsIn(String queue) {
        return JDBC.sql("SELECT count(*) FROM jobs WHERE queue = :queue").param("queue", queue)
                .query(Integer.class).single();
    }
}

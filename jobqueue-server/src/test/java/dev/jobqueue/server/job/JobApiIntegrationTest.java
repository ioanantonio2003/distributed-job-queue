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
import org.springframework.test.web.servlet.MvcResult;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class JobApiIntegrationTest extends PostgresIntegrationTest {

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    MockMvc mvc;

    @Test
    void shouldCreateQueuedJobAndReadItBackWhenRequestIsValid() throws Exception {
        MvcResult created = mvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"queue":"emails","type":"send-welcome","payload":{"to":"a@b.c"},
                                 "priority":2,"maxAttempts":7,"timeoutSeconds":30}
                                """))
                .andExpect(status().isCreated())
                .andReturn();
        String id = JsonPath.read(created.getResponse().getContentAsString(), "$.id");

        mvc.perform(get("/jobs/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id))
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andExpect(jsonPath("$.queue").value("emails"))
                .andExpect(jsonPath("$.type").value("send-welcome"))
                .andExpect(jsonPath("$.payload.to").value("a@b.c"))
                .andExpect(jsonPath("$.priority").value(2))
                .andExpect(jsonPath("$.maxAttempts").value(7))
                .andExpect(jsonPath("$.timeoutSeconds").value(30))
                .andExpect(jsonPath("$.attempts").value(0));
    }

    @Test
    void shouldApplyDefaultsWhenOptionalFieldsAreMissing() throws Exception {
        mvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"queue\":\"q\",\"type\":\"t\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.priority").value(0))
                .andExpect(jsonPath("$.maxAttempts").value(5))
                .andExpect(jsonPath("$.timeoutSeconds").value(60))
                .andExpect(jsonPath("$.payload").isEmpty());
    }

    @Test
    void shouldReturnLocationHeaderPointingToTheNewJobWhenCreated() throws Exception {
        MvcResult created = mvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"queue\":\"q\",\"type\":\"t\"}"))
                .andExpect(status().isCreated())
                .andReturn();

        String id = JsonPath.read(created.getResponse().getContentAsString(), "$.id");
        assertThat(created.getResponse().getHeader("Location")).endsWith("/jobs/" + id);
    }

    @Test
    void shouldReturnProblemDetailWithFieldErrorsWhenQueueIsBlank() throws Exception {
        mvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"queue\":\"\",\"type\":\"t\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.errors.queue").exists());
    }

    @Test
    void shouldReturnBadRequestWhenMaxAttemptsIsAboveLimit() throws Exception {
        mvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"queue\":\"q\",\"type\":\"t\",\"maxAttempts\":51}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errors.maxAttempts").exists());
    }

    @Test
    void shouldReturnBadRequestWhenPayloadIsNotAnObject() throws Exception {
        mvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"queue\":\"q\",\"type\":\"t\",\"payload\":\"text\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.detail").value("payload must be a JSON object"));
    }

    @Test
    void shouldReturnBadRequestWhenPayloadIsLargerThanLimit() throws Exception {
        String bigValue = "x".repeat(JobService.MAX_PAYLOAD_BYTES);

        mvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"queue\":\"q\",\"type\":\"t\",\"payload\":{\"data\":\"" + bigValue + "\"}}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value("payload must be at most 65536 bytes"));
    }

    @Test
    void shouldReturnBadRequestWhenBodyIsNotValidJson() throws Exception {
        mvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{not json"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON));
    }

    @Test
    void shouldReturnNotFoundProblemDetailWhenJobDoesNotExist() throws Exception {
        mvc.perform(get("/jobs/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.status").value(404));
    }

    @Test
    void shouldReturnBadRequestWhenIdIsNotAUuid() throws Exception {
        mvc.perform(get("/jobs/{id}", "not-a-uuid"))
                .andExpect(status().isBadRequest());
    }
}

package dev.jobqueue.worker.handler;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.common.PermanentFailureException;
import dev.jobqueue.worker.demo.EchoHandler;
import dev.jobqueue.worker.demo.SleepHandler;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * No Docker needed: a small Spring context per test is enough to exercise discovery and invocation.
 */
class HandlerRegistryTest {

    record GreetPayload(String name) {
    }

    static class GreetingHandler {
        final List<String> calls = new ArrayList<>();

        @JobHandler("greet")
        public void greet(JobContext context, GreetPayload payload) {
            calls.add(context.type() + ":" + context.attempt() + ":" + payload.name());
        }

        @JobHandler("boom")
        public void boom(JobContext context, GreetPayload payload) throws IOException {
            throw new IOException("disk full");
        }
    }

    static class OtherGreetingHandler {
        @JobHandler("greet")
        public void greet(JobContext context, GreetPayload payload) {
        }
    }

    static class WrongSignatureHandler {
        @JobHandler("wrong")
        public void handle(GreetPayload payload) {
        }
    }

    private static JobContext context(String type, int attempt) {
        return new JobContext(UUID.randomUUID(), "default", type, attempt, null);
    }

    private static AnnotationConfigApplicationContext startContext(Class<?>... handlerClasses) {
        AnnotationConfigApplicationContext spring = new AnnotationConfigApplicationContext();
        spring.registerBean(ObjectMapper.class, () -> JsonMapper.builder().build());
        spring.register(HandlerRegistry.class);
        spring.register(handlerClasses);
        spring.refresh();
        return spring;
    }

    @Test
    void shouldInvokeHandlerWithContextAndDeserializedPayloadWhenTypeIsRegistered() throws Exception {
        try (var spring = startContext(GreetingHandler.class)) {
            HandlerRegistry registry = spring.getBean(HandlerRegistry.class);

            registry.invoke(context("greet", 2), "{\"name\":\"Ana\"}");

            assertThat(spring.getBean(GreetingHandler.class).calls).containsExactly("greet:2:Ana");
        }
    }

    @Test
    void shouldListRegisteredTypesWhenHandlersAreDiscovered() {
        try (var spring = startContext(GreetingHandler.class, EchoHandler.class, SleepHandler.class)) {
            assertThat(spring.getBean(HandlerRegistry.class).types())
                    .containsExactlyInAnyOrder("greet", "boom", "echo", "sleep");
        }
    }

    @Test
    void shouldThrowClearErrorWhenJobTypeIsUnknown() {
        try (var spring = startContext(GreetingHandler.class)) {
            HandlerRegistry registry = spring.getBean(HandlerRegistry.class);

            assertThatThrownBy(() -> registry.invoke(context("nope", 1), "{}"))
                    .isInstanceOf(UnknownJobTypeException.class)
                    .hasMessageContaining("'nope'");
        }
    }

    @Test
    void shouldFailAtStartupWhenTwoHandlersShareTheSameType() {
        assertThatThrownBy(() -> startContext(GreetingHandler.class, OtherGreetingHandler.class))
                .hasMessageContaining("Duplicate @JobHandler for type 'greet'");
    }

    @Test
    void shouldFailAtStartupWhenHandlerMethodHasWrongSignature() {
        assertThatThrownBy(() -> startContext(WrongSignatureHandler.class))
                .hasMessageContaining("void m(JobContext, Payload)");
    }

    @Test
    void shouldThrowPermanentFailureWhenPayloadDoesNotMatchHandlerType() {
        try (var spring = startContext(GreetingHandler.class)) {
            HandlerRegistry registry = spring.getBean(HandlerRegistry.class);

            assertThatThrownBy(() -> registry.invoke(context("greet", 1), "[1, 2, 3]"))
                    .isInstanceOf(PermanentFailureException.class)
                    .hasMessageContaining("Invalid payload for job type 'greet'");
        }
    }

    @Test
    void shouldRethrowTheHandlersOwnExceptionWhenHandlerFails() {
        try (var spring = startContext(GreetingHandler.class)) {
            HandlerRegistry registry = spring.getBean(HandlerRegistry.class);

            assertThatThrownBy(() -> registry.invoke(context("boom", 1), "{\"name\":\"x\"}"))
                    .isInstanceOf(IOException.class)
                    .hasMessage("disk full");
        }
    }

    @Test
    void shouldRunDemoHandlersWhenTheyAreRegistered() throws Exception {
        try (var spring = startContext(EchoHandler.class, SleepHandler.class)) {
            HandlerRegistry registry = spring.getBean(HandlerRegistry.class);

            registry.invoke(context("echo", 1), "{\"hello\":\"world\"}");
            registry.invoke(context("sleep", 1), "{\"millis\":1}");

            assertThatThrownBy(() -> registry.invoke(context("sleep", 1), "{\"millis\":-5}"))
                    .isInstanceOf(PermanentFailureException.class);
        }
    }
}

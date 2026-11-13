package dev.jobqueue.worker.handler;

import dev.jobqueue.common.JobContext;
import dev.jobqueue.common.JobHandler;
import dev.jobqueue.common.PermanentFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Maps a job type to the {@link JobHandler} method that runs it.
 *
 * <p>Invariant: at most one handler per type. Handlers are collected once, after all singletons exist,
 * and a duplicate type or a bad method signature fails the application at startup rather than at the
 * first job. The map is only written during startup and only read afterwards, so no locking is needed.
 */
@Component
public class HandlerRegistry implements SmartInitializingSingleton {

    private static final Logger log = LoggerFactory.getLogger(HandlerRegistry.class);

    private record RegisteredHandler(Object bean, Method method, Class<?> payloadType) {
    }

    private final ListableBeanFactory beanFactory;
    private final ObjectMapper objectMapper;
    private final Map<String, RegisteredHandler> handlers = new HashMap<>();

    public HandlerRegistry(ListableBeanFactory beanFactory, ObjectMapper objectMapper) {
        this.beanFactory = beanFactory;
        this.objectMapper = objectMapper;
    }

    /**
     * Called by Spring once every singleton bean has been created. Scans beans for @JobHandler methods.
     * Bean types are inspected without creating the beans; only beans that own a handler are fetched.
     */
    @Override
    public void afterSingletonsInstantiated() {
        for (String beanName : beanFactory.getBeanNamesForType(Object.class, false, false)) {
            Class<?> beanType = beanFactory.getType(beanName);
            if (beanType == null) {
                continue;
            }
            // getUserClass: if the bean is a CGLIB proxy, look at the class the developer wrote.
            for (Method method : ClassUtils.getUserClass(beanType).getDeclaredMethods()) {
                JobHandler annotation = method.getAnnotation(JobHandler.class);
                if (annotation != null) {
                    register(annotation.value(), beanFactory.getBean(beanName), method);
                }
            }
        }
        log.info("Registered {} job handler(s): {}", handlers.size(), new TreeSet<>(handlers.keySet()));
    }

    private void register(String type, Object bean, Method method) {
        if (type.isBlank()) {
            throw new IllegalStateException("@JobHandler type must not be blank: " + describe(method));
        }
        if (method.getReturnType() != void.class || method.getParameterCount() != 2
                || method.getParameterTypes()[0] != JobContext.class) {
            throw new IllegalStateException("@JobHandler method must be 'void m(JobContext, Payload)': "
                    + describe(method));
        }
        ReflectionUtils.makeAccessible(method);
        RegisteredHandler previous = handlers.put(type,
                new RegisteredHandler(bean, method, method.getParameterTypes()[1]));
        if (previous != null) {
            throw new IllegalStateException("Duplicate @JobHandler for type '" + type + "': "
                    + describe(previous.method()) + " and " + describe(method));
        }
    }

    public Set<String> types() {
        return Set.copyOf(handlers.keySet());
    }

    /**
     * Runs the handler for {@code context.type()} with the payload deserialized into the handler's parameter type.
     *
     * @throws UnknownJobTypeException   no handler for the type
     * @throws PermanentFailureException the payload cannot be turned into the handler's parameter type
     * @throws Exception                 whatever the handler itself throws, unwrapped
     */
    public void invoke(JobContext context, String payloadJson) throws Exception {
        RegisteredHandler handler = handlers.get(context.type());
        if (handler == null) {
            throw new UnknownJobTypeException(context.type());
        }
        Object payload;
        try {
            payload = objectMapper.readValue(payloadJson, handler.payloadType());
        } catch (JacksonException e) {
            // The same payload will never parse on a retry, so do not waste attempts on it.
            throw new PermanentFailureException(
                    "Invalid payload for job type '" + context.type() + "': " + e.getOriginalMessage(), e);
        }
        try {
            handler.method().invoke(handler.bean(), context, payload);
        } catch (InvocationTargetException e) {
            // Reflection wraps what the handler threw; callers care about the real exception.
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e; // an Error: do not hide it
        }
    }

    private static String describe(Method method) {
        return method.getDeclaringClass().getName() + "#" + method.getName();
    }
}

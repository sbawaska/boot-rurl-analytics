package io.projectriff.demo.rurlanalytics;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Function;

@SpringBootApplication
public class RurlAnalyticsApplication {

    @Autowired
	private StringRedisTemplate redisTemplate;

	@Bean
	public StringRedisTemplate stringRedisTemplate() {
		return new StringRedisTemplate(redisConnectionFactory());
	}

	@Bean
	public RedisConnectionFactory redisConnectionFactory() {
		// TODO parameterize this
		return new LettuceConnectionFactory(new RedisStandaloneConfiguration(
				"my-redis-master.default.svc.cluster.local"));
	}

	@Bean
	public Function<String, Boolean> updateDomainCount() {
	    return s -> {
            try {
                String domainName = getDomainNameFromUrl(s);
                redisTemplate.opsForZSet().incrementScore("topDomains", domainName, 1);
            } catch (URISyntaxException e) {
                e.printStackTrace();
                return Boolean.FALSE;
            }
            return Boolean.TRUE;
        };
    }

    private String getDomainNameFromUrl(String input) throws URISyntaxException {
        URI uri = new URI(getLongUrl(input));
        String domain = uri.getHost();
        return domain.startsWith("www.") ? domain.substring(4) : domain;
    }

    private String getHash(String input) {
        int splitIndex = input.indexOf(':');
        if (splitIndex < 0) {
            // delimiter not found, or delimiter is last
            throw new IllegalArgumentException("expected form key:value but was "+input);
        }
        return input.substring(0, splitIndex);
    }

    private String getLongUrl(String input) {
        int splitIndex = input.indexOf(':');
        if (splitIndex < 0 || splitIndex == toString().length() + 1) {
            // delimiter not found, or delimiter is last
            throw new IllegalArgumentException("expected form key:value but was "+input);
        }
        return input.substring(splitIndex + 1);
    }

    public static void main(String[] args) {
		SpringApplication.run(RurlAnalyticsApplication.class, args);
	}
}

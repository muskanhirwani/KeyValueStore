package com.example.keyValueStore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;


@RestController
@SpringBootApplication
public class KeyValueStoreApplication {
	private static final Map<String, KeyValue> store = new ConcurrentHashMap<>();
	public Queue<KeyValue> queue = new Queue<>() {
	};

	public static void main(String[] args) {
		SpringApplication.run(KeyValueStoreApplication.class, args);
	}

	@PostMapping("/set/{key}")
	public ResponseEntity<String> set(@PathVariable String key, @RequestBody KeyValue value,
									  @RequestParam(required = false) Integer expiry, @RequestParam(required = false) String condition) {
		if (condition != null && !"NX".equals(condition) && !"XX".equals(condition)) {
			return ResponseEntity.badRequest().body("Invalid condition");
		}

		KeyValue existing = store.get(key);
		if (existing != null && "NX".equals(condition)) {
			return ResponseEntity.status(HttpStatus.CONFLICT).body("Key already exists");
		}
		if (existing == null && "XX".equals(condition)) {
			return ResponseEntity.status(HttpStatus.CONFLICT).body("Key does not exist");
		}

		if (expiry != null) {
			value.setExpiry(System.currentTimeMillis() + expiry * 1000L);
		}

		store.put(key, value);
		return ResponseEntity.ok().build();
	}

	@GetMapping("/get/{key}")
	public ResponseEntity<KeyValue> get(@PathVariable String key) {
		KeyValue value = store.get(key);
		if (value == null) {
			return ResponseEntity.notFound().build();
		}
		if (value.isExpired()) {
			store.remove(key);
			return ResponseEntity.notFound().build();
		}
		return ResponseEntity.ok(value);
	}

	@PostMapping("/qpush/{key}")
	public ResponseEntity<String> qpush(@PathVariable String key, @RequestBody String[] values) {
		KeyValue existing = store.get(key);
		if (existing == null) {
			existing = new KeyValue();
			store.put(key, existing);
		}
		existing.push(values);
		return ResponseEntity.ok().build();
	}

	@GetMapping("/qpop/{key}")
	public ResponseEntity<String> qpop(@PathVariable String key) {
		KeyValue value = store.get(key);
		if (value == null) {
			return ResponseEntity.notFound().build();
		}
		String result = value.toString();
		if (result == null) {
			store.remove(key);
			return ResponseEntity.notFound().build();
		}
		return ResponseEntity.ok(result);
	}

	@GetMapping("/bqpop/{key}")
	public ResponseEntity<String> bqpop(@PathVariable String key, @RequestParam(defaultValue = "0") double timeout) throws InterruptedException {
		synchronized (store) {
			KeyValue value = store.get(key);
			if (value == null) {
				return ResponseEntity.notFound().build();
			}
			String result = value.toString();
			if (result != null) {
				return ResponseEntity.ok(result);
			}
			if (timeout == 0) {
				return ResponseEntity.notFound().build();
			}
			long start = System.currentTimeMillis();
			long duration = (long) (timeout * 1000);
			while (true) {
				store.wait(duration);
				value = store.get(key);
				if (value == null) {
					return ResponseEntity.notFound().build();
				}
				result = value.toString();


			}
		}
	}
}
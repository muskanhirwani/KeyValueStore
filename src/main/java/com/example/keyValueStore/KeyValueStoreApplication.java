package com.example.keyValueStore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;


@RestController
@SpringBootApplication
public class KeyValueStoreApplication {
	private static final Map<String, String> store = new ConcurrentHashMap<>();
	private static final Map<String, Queue<String>> queues = new ConcurrentHashMap<>();


	public static void main(String[] args) {
		SpringApplication.run(KeyValueStoreApplication.class, args);
	}

	@PostMapping("/set/{key}")
	public ResponseEntity<String> set(@PathVariable String key, @RequestBody String value,
									  @RequestParam(required = false) Integer expiry, @RequestParam(required = false) String condition) {
		if (expiry != null && expiry <= 0) {
			return ResponseEntity.badRequest().body("Invalid expiry value");
		}

		boolean keyExists = store.containsKey(key);
		if (condition != null && condition.equalsIgnoreCase("NX") && keyExists) {
			return ResponseEntity.status(HttpStatus.CONFLICT).body("Key already exists");
		}
		if (condition != null && condition.equalsIgnoreCase("XX") && !keyExists) {
			return ResponseEntity.status(HttpStatus.CONFLICT).body("Key does not exist");
		}

		store.put(key, value);
		if (expiry != null) {
			Thread t = new Thread(() -> {
				try {
					Thread.sleep(expiry * 1000L);
					store.remove(key);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			});
			t.start();
		}
		return ResponseEntity.ok().build();
	}

	@GetMapping("/get/{key}")
	public ResponseEntity<String> get(@PathVariable String key) {
		String value = store.get(key);
		if (value == null) {
			return ResponseEntity.notFound().build();
		}
		return ResponseEntity.ok(value);
	}

	@PostMapping("/qpush/{key}")
	public ResponseEntity<String> qpush(@PathVariable String key, @RequestBody String[] values) {
		queues.putIfAbsent(key, new LinkedList<String>());
		Queue<String> queue = queues.get(key);
		for (String value : values) {
			queue.offer(value);
		}
		return ResponseEntity.ok().build();
	}

	@GetMapping("/qpop/{key}")
	public ResponseEntity<String> qpop(@PathVariable String key) {
		Queue<String> queue = queues.get(key);
		if (queue == null) {
			return ResponseEntity.notFound().build();
		}
		String value = queue.poll();
		if (value == null) {
			return ResponseEntity.notFound().build();
		}
		return ResponseEntity.ok(value);
	}

	@GetMapping("/bqpop/{key}")
	public ResponseEntity<String> bqpop(@PathVariable String key, @RequestParam(defaultValue = "0") double timeout) throws InterruptedException {
		Queue<String> queue = queues.get(key);
		if (queue == null) {
			return ResponseEntity.notFound().build();
		}
		synchronized (queue) {
			if (queue.isEmpty()) {
				try {
					queue.wait((long) (timeout * 1000L));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if (queue.isEmpty()) {
				return ResponseEntity.notFound().build();
			}
			String value = queue.poll();
			return ResponseEntity.ok(value);
		}


		}
	}

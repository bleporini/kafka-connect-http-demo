/**
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.http.controller;

import io.confluent.connect.http.domain.Message;
import io.confluent.connect.http.repository.MessageRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.toList;

@RestController
@Slf4j
public class MessageController {

  private final MessageRepository messageRepository;

  private final AtomicBoolean shouldRaiseException = new AtomicBoolean(false);

  public MessageController(MessageRepository messageRepository) {
    this.messageRepository = messageRepository;
  }

  @PostMapping("/config/respondAlwaysError")
  public void postRespondAlwaysError() {
    log.warn("Now all endpoints are throwing an exception");
    shouldRaiseException.set(true);
  }

  @PostMapping("/config/respondNormal")
  public void postRespondNormal() {
    log.info("Back to normal operations without unconditional errors");
    shouldRaiseException.set(false);
  }

  private void checkError() {
    if(shouldRaiseException.get())
      throw new RuntimeException("Generated error at " + currentTimeMillis());
  }

  @PutMapping(path = "/api/messages")
  public ResponseEntity putMessage(@RequestBody String message) {
    checkError();
    return ResponseEntity
        .status(HttpStatus.CREATED)
        .body(save(message));
  }

  @PostMapping(path = "/api/messages")
  public ResponseEntity createMessage(@RequestBody String message) {
    checkError();
    return ResponseEntity
        .status(HttpStatus.CREATED)
        .body(save(message));
  }

  @DeleteMapping(path = "/api/messages/{id}")
  public ResponseEntity deleteMessage(@PathVariable String id) {
    checkError();
    log.warn("DELETING MESSAGE WITH ID: {}", id);

    try {
      Long lId = Long.valueOf(id);
      messageRepository.deleteById(lId);
    } catch (Exception e) {
      log.error("Unable to convert {} into a Long", id);
    }

    return ResponseEntity
        .status(HttpStatus.OK)
        .build();
  }

  @PostMapping(path = "/api/messages/batch")
  public ResponseEntity createMessages(@RequestBody List<String> messages) {
    checkError();
    log.info("MESSAGES RECEIVED: {}", messages);

    // convert to Message
    List<Message> messagesToSave = messages
        .stream()
        .map(Message::new)
        .collect(toList());

    // save
    Iterable<Message> savedMessages = messageRepository.saveAll(messagesToSave);

    // return saved messages (with ID generated)
    return ResponseEntity
        .status(HttpStatus.CREATED)
        .body(savedMessages);
  }

  @GetMapping(path = "/api/messages")
  public Iterable<Message> getMessages() {
    return messageRepository.findAll();
  }

  private Message save(String message) {
    log.info("MESSAGE RECEIVED: {}", message);
    return messageRepository.save(new Message(message));
  }
}

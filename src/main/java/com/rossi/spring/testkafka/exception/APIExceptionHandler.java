package com.rossi.spring.testkafka.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.validation.BindException;
import org.springframework.validation.Validator;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.ResponseStatus;

@Slf4j
@ControllerAdvice
public class APIExceptionHandler {
    @Autowired
    private Validator validator;

    @InitBinder
    protected void initBinder(WebDataBinder binder) {
        binder.setValidator(validator);
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ResponseEntity<?> handleUnknownException (Exception ex){
        log.error("Handle Unknown Exception => {} ...", ex);
        return ResponseEntity.status(HttpStatus.OK).body("AN ERROR OCCURRED");
    }

    @ExceptionHandler(BindException.class)
    public ResponseEntity<?> handleException (BindException ex){
        log.error("Handle Bind Exception => {} ...", ex);
        return ResponseEntity.status(HttpStatus.OK).body("INSUFFICIENT PARAM");
    }

    @ExceptionHandler(ListenerExecutionFailedException.class)
    public ResponseEntity<?> handleException (ListenerExecutionFailedException ex){
        log.error("Handle Exception => {} ...", ex);
        return ResponseEntity.status(HttpStatus.OK).body("AN ERROR OCCURRED");
    }

    @ExceptionHandler(MessageConversionException.class)
    public ResponseEntity<?> handleException (MessageConversionException ex){
        log.error("Handle Exception => {} ...", ex);
        return ResponseEntity.status(HttpStatus.OK).body("AN ERROR OCCURRED");
    }
}

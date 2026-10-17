package ru.datacollector.api;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import ru.datacollector.common.api.AuthApi;
import ru.datacollector.common.model.AuthRequest;
import ru.datacollector.common.model.UserResponse;
import ru.datacollector.service.AuthService;

@RequiredArgsConstructor
@RestController
public class AuthController implements AuthApi {

    private final AuthService authService;

    @Override
    public ResponseEntity<UserResponse> register(AuthRequest body) {
        return null;
    }

    @Override
    public ResponseEntity<UserResponse> login(AuthRequest body) {
        return null;
    }

    @Override
    public ResponseEntity<Void> logout() {
        return null;
    }

    @Override
    public ResponseEntity<UserResponse> getCurrentUser() {
        return null;
    }
}

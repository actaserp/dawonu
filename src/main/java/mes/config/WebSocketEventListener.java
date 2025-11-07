package mes.config;

import org.springframework.context.event.EventListener;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.messaging.*;

@Component
public class WebSocketEventListener {

    // 연결 성공 시
    @EventListener
    public void handleWebSocketConnectListener(SessionConnectedEvent event) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(event.getMessage());
        System.out.println("✅ WebSocket 연결 성공: sessionId = " + accessor.getSessionId());
    }

    // 연결 해제 시
    @EventListener
    public void handleWebSocketDisconnectListener(SessionDisconnectEvent event) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(event.getMessage());
        System.out.println("❌ WebSocket 연결 종료: sessionId = " + accessor.getSessionId());
    }

    // 구독 이벤트
    @EventListener
    public void handleWebSocketSubscribeListener(SessionSubscribeEvent event) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(event.getMessage());
        System.out.println("📡 구독 발생: destination = " + accessor.getDestination());
    }

    // 구독 해제 이벤트
    @EventListener
    public void handleWebSocketUnsubscribeListener(SessionUnsubscribeEvent event) {
        StompHeaderAccessor accessor = StompHeaderAccessor.wrap(event.getMessage());
        System.out.println("🧹 구독 취소: sessionId = " + accessor.getSessionId());
    }
}
package org.example.emsibot.agents;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.MessageChatMemoryAdvisor;
import org.springframework.ai.chat.memory.ChatMemory;
import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class AIAgent {

    private ChatClient chatClient;

    public AIAgent(ChatClient.Builder builder, ChatMemory memory, ToolCallbackProvider tools) {
        Arrays.stream(tools.getToolCallbacks()).forEach(toolCallback -> {
            System.out.println("------------------------");
            System.out.println(toolCallback.getToolDefinition());
        });
        this.chatClient = builder
                .defaultSystem("""
                        Vous etes un assistant qui se charge de repondre au questions des utilisateur
                        en fonction du contexte fourni.
                        Si aucun contexte n'est fourni reponds avec JE SAIS PAS.
                        """)
                .defaultAdvisors(
                        MessageChatMemoryAdvisor.builder(memory).build()
                )
                .defaultToolCallbacks(tools)

                .build();
    }

    public String askAgent(String query ){
        return chatClient.prompt()
                .user(query)
                .call().content();
    }
}

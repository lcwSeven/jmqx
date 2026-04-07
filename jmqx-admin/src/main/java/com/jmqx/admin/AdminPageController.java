package com.jmqx.admin;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
@Controller
public class AdminPageController {
    @GetMapping({"/", "/admin", "/admin/**"})
    public String index() {
        return "forward:/index.html";
    }
}

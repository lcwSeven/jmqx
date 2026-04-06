package com.jmqx.admin;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author liucaiwen
 * @date 2026/4/5
 */
@RestController
public class AdminPageController {
    @GetMapping(value = {"/", "/admin"}, produces = MediaType.TEXT_HTML_VALUE)
    public String index() {
        return """
            <!doctype html>
            <html lang="zh-CN">
            <head>
              <meta charset="UTF-8" />
              <meta name="viewport" content="width=device-width, initial-scale=1.0" />
              <title>JMQX Admin</title>
              <style>
                body { font-family: Arial, sans-serif; margin: 40px; line-height: 1.6; color: #222; }
                a { color: #0b57d0; text-decoration: none; }
                a:hover { text-decoration: underline; }
                code { background: #f5f5f5; padding: 2px 6px; border-radius: 4px; }
              </style>
            </head>
            <body>
              <h2>JMQX Admin Backend</h2>
              <p>管理后台后端已启动。</p>
              <p>状态接口：<a href="/api/admin/status" target="_blank">/api/admin/status</a></p>
              <p>配置接口：<code>POST /api/admin/config</code></p>
              <p>如果你在使用 React 前端，请在 <code>jmqx-admin/frontend</code> 执行 <code>npm run dev</code> 后访问前端地址。</p>
            </body>
            </html>
            """;
    }
}

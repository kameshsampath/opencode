import { Effect } from "effect"
import { PluginV2 } from "../../plugin"
import { ProviderV2 } from "../../provider"

type FetchLike = (url: string | URL | Request, init?: RequestInit) => Promise<Response>
const providerID = ProviderV2.ID.make("snowflake-cortex")

function snowflakeBaseURL(account: string) {
  return `https://${account}.snowflakecomputing.com/api/v2/cortex/v1`
}

function stringOption(options: Record<string, unknown>, key: string) {
  return typeof options[key] === "string" ? options[key] : undefined
}

export function cortexBaseURL(options: Record<string, unknown>) {
  const account = process.env.SNOWFLAKE_ACCOUNT ?? stringOption(options, "account")
  const baseURL = stringOption(options, "baseURL")
  if (baseURL && account) return baseURL.replaceAll("${SNOWFLAKE_ACCOUNT}", account)
  if (baseURL) return baseURL
  if (account) return snowflakeBaseURL(account)
  return undefined
}

// Exported for testing: intercepts Cortex-specific request/response quirks.
export function cortexFetch(upstream: FetchLike = fetch) {
  return async (url: string | URL | Request, init?: RequestInit): Promise<Response> => {
    if (init?.body && typeof init.body === "string") {
      try {
        const body = JSON.parse(init.body)
        if ("max_tokens" in body) {
          body.max_completion_tokens = body.max_tokens
          delete body.max_tokens
          init = { ...init, body: JSON.stringify(body) }
        }
      } catch {}
    }

    const response = await upstream(url, init)

    // Cortex returns 400 "conversation complete" as a normal stop condition
    if (!response.ok && response.status === 400) {
      try {
        const errorData = (await response.clone().json()) as Record<string, unknown>
        if (
          String(errorData.message || errorData.error || "")
            .toLowerCase()
            .includes("conversation complete")
        ) {
          return new Response(
            JSON.stringify({ choices: [{ finish_reason: "stop", message: { content: "", role: "assistant" } }] }),
            { status: 200, headers: new Headers({ "content-type": "application/json" }) },
          )
        }
      } catch {}
    }

    if (response.body && response.headers.get("content-type")?.includes("text/event-stream"))
      return rewriteCortexStream(response)

    return response
  }
}

export const SnowflakeCortexPlugin = PluginV2.define({
  id: providerID,
  effect: Effect.gen(function* () {
    return {
      "aisdk.sdk": Effect.fn(function* (evt) {
        if (evt.model.providerID !== providerID) return
        const pat =
          process.env.SNOWFLAKE_CORTEX_PAT ?? (typeof evt.options.apiKey === "string" ? evt.options.apiKey : undefined)
        const upstream = typeof evt.options.fetch === "function" ? (evt.options.fetch as FetchLike) : undefined
        if (evt.options.includeUsage !== false) evt.options.includeUsage = true
        const mod = yield* Effect.promise(() => import("@ai-sdk/openai-compatible"))
        const baseURL = cortexBaseURL(evt.options)
        if (baseURL) evt.options.baseURL = baseURL
        evt.sdk = mod.createOpenAICompatible({
          ...evt.options,
          ...(baseURL ? { baseURL } : {}),
          ...(pat ? { apiKey: pat } : {}),
          fetch: cortexFetch(upstream) as typeof fetch,
        } as any)
      }),
    }
  }),
})

function rewriteCortexStream(response: Response) {
  if (!response.body) return response
  const reader = response.body.getReader()
  const encoder = new TextEncoder()
  const decoder = new TextDecoder()
  let buffer = ""
  const stream = new ReadableStream({
    async pull(ctrl) {
      const { done, value } = await reader.read()
      if (done) {
        const text = buffer + decoder.decode()
        if (text) ctrl.enqueue(encoder.encode(rewriteRole(text)))
        ctrl.close()
        return
      }
      buffer += decoder.decode(value, { stream: true })
      const index = buffer.lastIndexOf("\n")
      if (index === -1) return
      ctrl.enqueue(encoder.encode(rewriteRole(buffer.slice(0, index + 1))))
      buffer = buffer.slice(index + 1)
    },
    cancel() {
      reader.cancel()
    },
  })
  return new Response(stream, { headers: response.headers, status: response.status })
}

function rewriteRole(text: string) {
  return text.replace(/"role"\s*:\s*""/g, '"role":"assistant"')
}

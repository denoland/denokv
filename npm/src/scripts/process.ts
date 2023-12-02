import { TextLineStream } from "https://deno.land/std@0.208.0/streams/text_line_stream.ts";

type Opts = {
  command: string;
  cwd?: string;
  args: string[];
  stdinFn?: (stream: WritableStream<Uint8Array>) => Promise<void>;
  stdoutFn?: (stream: ReadableStream<Uint8Array>) => Promise<void>;
  signal?: AbortSignal;
};

export async function run(
  { command, cwd, args, stdinFn, stdoutFn, signal }: Opts,
): Promise<{ code: number; millis: number }> {
  const start = Date.now();
  const cmd = new Deno.Command(command, {
    cwd,
    args,
    env: {
      NO_COLOR: "1",
    },
    stderr: "piped",
    stdout: "piped",
    stdin: stdinFn ? "piped" : undefined,
    signal,
  });
  const p = cmd.spawn();
  const errorLines: string[] = [];
  const readStderr = async () => {
    for await (
      const line of p.stderr.pipeThrough(new TextDecoderStream()).pipeThrough(
        new TextLineStream(),
      )
    ) {
      console.error(line);
      errorLines.push(line);
    }
  };
  const readStdout = async () => {
    if (stdoutFn) {
      await stdoutFn(p.stdout);
    } else {
      for await (
        const line of p.stdout.pipeThrough(new TextDecoderStream()).pipeThrough(
          new TextLineStream(),
        )
      ) {
        console.log(line);
      }
    }
  };
  const writeStdout = async () => {
    if (!stdinFn) return;
    await stdinFn(p.stdin);
  };
  await Promise.all([readStderr(), readStdout(), writeStdout()]);
  const { code, success } = await p.status;
  if (!success) {
    throw new Error(errorLines.join("\n"));
  }
  return { code, millis: Date.now() - start };
}

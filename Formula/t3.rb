class T3 < Formula
  desc "t3 (Turbo 3): Blazing-fast parallel S3 client and drop-in s5cmd replacement in Rust"
  homepage "https://ehlerst.github.io/t3/"
  version "0.1.2"
  license any_of: ["Apache-2.0", "MIT"]

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/ehlerst/t3/releases/download/v#{version}/t3-darwin-arm64.tar.gz"
    else
      url "https://github.com/ehlerst/t3/releases/download/v#{version}/t3-darwin-amd64.tar.gz"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/ehlerst/t3/releases/download/v#{version}/t3-linux-arm64.tar.gz"
    else
      url "https://github.com/ehlerst/t3/releases/download/v#{version}/t3-linux-amd64.tar.gz"
    end
  end

  def install
    bin.install "t3"
    (bash_completion/"t3").write Utils.safe_popen_read(bin/"t3", "--install-completion", "bash")
    (zsh_completion/"_t3").write Utils.safe_popen_read(bin/"t3", "--install-completion", "zsh")
    (fish_completion/"t3.fish").write Utils.safe_popen_read(bin/"t3", "--install-completion", "fish")
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/t3 version")
  end
end

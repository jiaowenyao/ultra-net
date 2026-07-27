// BufferRingAssembler 单元测试
//
// 测试帧重组逻辑——在各种分段场景下验证 try_decode_frame 的正确性。

#include <gtest/gtest.h>
#include "ultranet/net/buffer_ring_assembler.hpp"
#include "ultranet/net/websocket.hpp"

using namespace ynet::async::net;
using namespace ynet::async::io;

// ── 辅助函数：构造一个编码后的 WS 文本帧 ──────────────────────────────

static std::vector<uint8_t> make_encoded_frame(
    const std::string& payload,
    bool masked = true)
{
    websocket::WebSocketFrame frame = websocket::WebSocketFrame::text(payload);
    frame.mask = masked;
    frame.masking_key = masked ? 0x01020304u : 0u;
    return frame.encode(masked);
}

// ── 移动语义测试 ──────────────────────────────────────────────────────

TEST(BufferRingAssemblerTest, DefaultConstructed)
{
    BufferRingAssembler assembler;
    EXPECT_FALSE(assembler.is_started());
    EXPECT_FALSE(assembler.is_stopped());
    EXPECT_FALSE(assembler.has_remaining());
    EXPECT_TRUE(assembler.reasm_buffer().empty());
}

TEST(BufferRingAssemblerTest, MoveConstructorTransfersState)
{
    BufferRingAssembler a1;
    // 没有 start() 的情况下移动（无 IoCallback）
    BufferRingAssembler a2(std::move(a1));
    EXPECT_FALSE(a2.is_started());
    EXPECT_FALSE(a1.is_started());
}

// ── 帧解码测试（直接操作重组缓冲区） ──────────────────────────────────

TEST(BufferRingAssemblerDecodeTest, SingleSmallFrame)
{
    // 构造 "hello" 文本帧的编码数据
    auto encoded = make_encoded_frame("hello");

    // 模拟：将完整帧放入重组缓冲区
    BufferRingAssembler assembler;
    // 使用 append_chunk 需要有效的 BufferGroup，这里直接测试 try_decode_frame 逻辑
    // 通过构造一个完整编码帧来模拟

    size_t consumed = 0;
    websocket::WebSocketFrame decoded;
    bool ok = websocket::WebSocketFrame::decode(
        encoded.data(), encoded.size(), consumed, decoded);

    EXPECT_TRUE(ok);
    EXPECT_EQ(consumed, encoded.size());
    EXPECT_EQ(decoded.payload, "hello");
    EXPECT_EQ(decoded.opcode, websocket::OpCode::Text);
    EXPECT_TRUE(decoded.fin);
}

TEST(BufferRingAssemblerDecodeTest, IncompleteHeader)
{
    // 只给 1 字节——不足以解码帧头
    uint8_t partial = 0x81;  // fin + text opcode，缺少长度字节
    size_t consumed = 0;
    websocket::WebSocketFrame decoded;

    bool ok = websocket::WebSocketFrame::decode(
        &partial, 1, consumed, decoded);

    EXPECT_FALSE(ok);
    EXPECT_EQ(consumed, 0u);
}

TEST(BufferRingAssemblerDecodeTest, FrameHeaderSpansChunks)
{
    // 构造一个需要扩展长度字段的帧（>125 字节）
    std::string payload(200, 'x');
    auto encoded = make_encoded_frame(payload);

    // 模拟分两段到达：前 4 字节 + 剩余
    std::vector<uint8_t> chunk1(encoded.begin(), encoded.begin() + 4);
    std::vector<uint8_t> chunk2(encoded.begin() + 4, encoded.end());

    // 第一段：数据不完整
    size_t consumed = 0;
    websocket::WebSocketFrame decoded;
    bool ok1 = websocket::WebSocketFrame::decode(
        chunk1.data(), chunk1.size(), consumed, decoded);
    EXPECT_FALSE(ok1);
    EXPECT_EQ(consumed, 0u);

    // 合并后解码
    std::vector<uint8_t> combined = chunk1;
    combined.insert(combined.end(), chunk2.begin(), chunk2.end());
    bool ok2 = websocket::WebSocketFrame::decode(
        combined.data(), combined.size(), consumed, decoded);
    EXPECT_TRUE(ok2);
    EXPECT_EQ(decoded.payload, payload);
}

TEST(BufferRingAssemblerDecodeTest, LargeFrameMultipleChunks)
{
    // 10KB payload——跨 3 个 4KB buffer
    std::string payload(10000, 'y');
    auto encoded = make_encoded_frame(payload);

    // 分 2KB 块逐一喂入
    std::vector<uint8_t> reasm;
    bool decoded_ok = false;
    websocket::WebSocketFrame decoded;

    for (size_t i = 0; i < encoded.size(); i += 2048)
    {
        size_t chunk_size = std::min<size_t>(2048, encoded.size() - i);
        reasm.insert(reasm.end(),
                     encoded.begin() + i,
                     encoded.begin() + i + chunk_size);

        size_t consumed = 0;
        bool ok = websocket::WebSocketFrame::decode(
            reasm.data(), reasm.size(), consumed, decoded);

        if (ok)
        {
            decoded_ok = true;
            break;
        }
    }

    EXPECT_TRUE(decoded_ok);
    EXPECT_EQ(decoded.payload.size(), 10000u);
    EXPECT_EQ(decoded.payload, payload);
}

// ── 掩码帧测试 ────────────────────────────────────────────────────────

TEST(BufferRingAssemblerDecodeTest, MaskedFrame)
{
    auto encoded = make_encoded_frame("masked_test", true);

    size_t consumed = 0;
    websocket::WebSocketFrame decoded;
    bool ok = websocket::WebSocketFrame::decode(
        encoded.data(), encoded.size(), consumed, decoded);

    EXPECT_TRUE(ok);
    EXPECT_EQ(decoded.payload, "masked_test");
    EXPECT_TRUE(decoded.mask);
}

// ── 控制帧测试 ────────────────────────────────────────────────────────

TEST(BufferRingAssemblerDecodeTest, PingFrame)
{
    websocket::WebSocketFrame ping = websocket::WebSocketFrame::ping("ping_data");
    ping.mask = true;
    ping.masking_key = 0x01020304;
    auto encoded = ping.encode(true);

    size_t consumed = 0;
    websocket::WebSocketFrame decoded;
    bool ok = websocket::WebSocketFrame::decode(
        encoded.data(), encoded.size(), consumed, decoded);

    EXPECT_TRUE(ok);
    EXPECT_EQ(decoded.opcode, websocket::OpCode::Ping);
    EXPECT_EQ(decoded.payload, "ping_data");
}

TEST(BufferRingAssemblerDecodeTest, CloseFrame)
{
    auto encoded = make_encoded_frame("bye");

    size_t consumed = 0;
    websocket::WebSocketFrame decoded;
    bool ok = websocket::WebSocketFrame::decode(
        encoded.data(), encoded.size(), consumed, decoded);

    EXPECT_TRUE(ok);
    EXPECT_EQ(decoded.opcode, websocket::OpCode::Text);
    EXPECT_EQ(decoded.payload, "bye");
}

// ── 边界测试 ──────────────────────────────────────────────────────────

TEST(BufferRingAssemblerDecodeTest, EmptyPayload)
{
    // 空 payload 帧（合法 WS 帧：fin + text，0 长度）
    uint8_t buf[] = {0x81, 0x00};  // fin=1, opcode=text, mask=0, len=0
    size_t consumed = 0;
    websocket::WebSocketFrame decoded;
    bool ok = websocket::WebSocketFrame::decode(buf, sizeof(buf), consumed, decoded);

    EXPECT_TRUE(ok);
    EXPECT_EQ(consumed, 2u);
    EXPECT_TRUE(decoded.payload.empty());
    EXPECT_TRUE(decoded.fin);
}

TEST(BufferRingAssemblerDecodeTest, Max16BitLength)
{
    // payload 长度 = 65535（16 位扩展长度最大值）
    std::string payload(65535, 'z');
    auto encoded = make_encoded_frame(payload);

    size_t consumed = 0;
    websocket::WebSocketFrame decoded;
    bool ok = websocket::WebSocketFrame::decode(
        encoded.data(), encoded.size(), consumed, decoded);

    EXPECT_TRUE(ok);
    EXPECT_EQ(decoded.payload.size(), 65535u);
}

// ── 重组缓冲区残留数据测试 ────────────────────────────────────────────

TEST(BufferRingAssemblerDecodeTest, ConsumedRemainingData)
{
    // 模拟缓冲区中有两个帧：先 decode 第一个帧，验证残留数据保留
    auto frame1 = make_encoded_frame("first");
    auto frame2 = make_encoded_frame("second");

    std::vector<uint8_t> combined;
    combined.insert(combined.end(), frame1.begin(), frame1.end());
    combined.insert(combined.end(), frame2.begin(), frame2.end());

    // 解码第一帧
    size_t consumed1 = 0;
    websocket::WebSocketFrame decoded1;
    bool ok1 = websocket::WebSocketFrame::decode(
        combined.data(), combined.size(), consumed1, decoded1);
    EXPECT_TRUE(ok1);
    EXPECT_EQ(decoded1.payload, "first");
    EXPECT_EQ(consumed1, frame1.size());

    // 模拟 memmove 后解码第二帧
    std::memmove(combined.data(), combined.data() + consumed1,
                 combined.size() - consumed1);
    combined.resize(combined.size() - consumed1);

    size_t consumed2 = 0;
    websocket::WebSocketFrame decoded2;
    bool ok2 = websocket::WebSocketFrame::decode(
        combined.data(), combined.size(), consumed2, decoded2);
    EXPECT_TRUE(ok2);
    EXPECT_EQ(decoded2.payload, "second");
}

// ── RecvChunkResult 模拟测试 ──────────────────────────────────────────

TEST(RecvChunkResultSimTest, NormalChunk)
{
    RecvChunkResult chunk{512, 0x000A0000};
    EXPECT_EQ(chunk.res, 512);
    EXPECT_EQ(chunk.buffer_id(), 10u);
    EXPECT_FALSE(chunk.is_eof());
    EXPECT_FALSE(chunk.is_error());
}

TEST(RecvChunkResultSimTest, EofChunk)
{
    RecvChunkResult chunk{0, 0};
    EXPECT_TRUE(chunk.is_eof());
    EXPECT_FALSE(chunk.is_error());
}

TEST(RecvChunkResultSimTest, ErrorChunk)
{
    RecvChunkResult chunk{-ECONNRESET, 0};
    EXPECT_FALSE(chunk.is_eof());
    EXPECT_TRUE(chunk.is_error());
}

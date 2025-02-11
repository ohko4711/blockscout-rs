cat <<EOF | docker exec -i gn-postgres-dev psql -U graph-node -d graph-node

INSERT INTO sgd1.domain (
  block_range,
  id,
  name,
  label_name,
  labelhash,
  parent,
  subdomain_count,
  resolved_address,
  resolver,
  ttl,
  is_migrated,
  created_at,
  owner,
  registrant,
  wrapped_owner,
  expiry_date
)
VALUES (
  '[0,2147483647)'::int4range,  -- 指定有效区块范围（根据实际情况调整）
  '0xc3a2b194615fe15824a8c25f10cb10180b5f20e68643314a4bbbf71a88c1ac2a', -- id: TLD 的 namehash
  'ace',  -- name: 显示名称
  'ace',  -- label_name: 标签名称
  decode('ec08d0974e93cdef3f47cd5c5a287a626489bcf52960b0af6a2544794055c85b', 'hex'), -- labelhash: keccak256("ace")
  NULL,   -- parent: 顶级域无父域
  0,      -- subdomain_count: 初始无子域
  NULL,   -- resolved_address: 暂无解析地址
  NULL,   -- resolver: 暂无解析器
  0,      -- ttl: 默认 0
  false,  -- is_migrated: 未迁移
  1683442174, -- created_at: 创建时间（Unix 时间戳）
  '0x0000000000000000000000000000000000000000', -- owner: 示例拥有者地址（请根据实际情况替换）
  NULL,   -- registrant: 暂无注册者
  NULL,   -- wrapped_owner: 暂无包装域拥有者
  NULL    -- expiry_date: 暂无到期时间
);

EOF
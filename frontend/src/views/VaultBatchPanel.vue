<template>
  <section class="vault-panel" data-testid="vault-batch-panel">
    <header class="vault-panel-head">
      <div>
        <h3>Vault 批次清单</h3>
        <p class="page-desc">每次批量加载都会生成批次清单，逐行可追到源行、哈希键和输入判定。</p>
      </div>
      <button class="btn" type="button" data-testid="vault-refresh" @click="reload">刷新批次</button>
    </header>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <table class="data-table">
      <thead>
        <tr>
          <th>批次号</th>
          <th>模型</th>
          <th>记录源</th>
          <th>上报</th>
          <th>新增</th>
          <th>重复提交</th>
          <th>更正</th>
          <th>拒绝</th>
          <th>状态</th>
          <th>清单</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="batch in batches"
          :key="batch.批次号"
          :data-testid="`vault-batch-${batch.批次号}`"
        >
          <td>{{ batch.批次号 }}</td>
          <td>{{ batch.模型名称 }}</td>
          <td>{{ batch.记录源 }}</td>
          <td>{{ batch.上报行数 }}</td>
          <td>{{ batch.新增行数 }}</td>
          <td>{{ batch.重复行数 }}</td>
          <td>{{ batch.更正行数 }}<span v-if="batch.迟到更正行数">（迟到 {{ batch.迟到更正行数 }}）</span></td>
          <td>{{ batch.拒绝行数 }}</td>
          <td>{{ batch.状态 }}</td>
          <td>
            <button
              class="link"
              type="button"
              :data-testid="`vault-batch-open-${batch.批次号}`"
              @click="toggle(batch.批次号)"
            >
              {{ openBatch === batch.批次号 ? '收起' : '查看明细' }}
            </button>
          </td>
        </tr>
        <tr v-if="!batches.length">
          <td colspan="10" class="empty-state">暂无批次清单，可在 Vault 模型行执行「执行加载」</td>
        </tr>
      </tbody>
    </table>

    <div v-if="detail" class="vault-detail" :data-testid="`vault-detail-${detail.批次号}`">
      <p class="vault-summary">{{ detail.清单摘要 }}</p>
      <table class="data-table">
        <thead>
          <tr>
            <th>源行标识</th>
            <th>业务键</th>
            <th>输入类型</th>
            <th>Hub状态</th>
            <th>Link状态</th>
            <th>Satellite版本</th>
            <th>旧值</th>
            <th>新值</th>
            <th>拒绝原因</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="row in detail.清单"
            :key="row.id"
            :data-testid="`vault-row-${row.源行标识}`"
            :data-input-type="row.输入类型"
          >
            <td>{{ row.源行标识 }}</td>
            <td>{{ row.业务键 || '—' }}</td>
            <td>{{ row.输入类型 }}</td>
            <td>{{ row.Hub装载状态 }}</td>
            <td>{{ row.Link装载状态 }}</td>
            <td>{{ row.Satellite版本 || '—' }}</td>
            <td>{{ row.旧值 || '—' }}</td>
            <td>{{ row.新值 || '—' }}</td>
            <td>{{ row.拒绝原因 || '—' }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type BatchRow = {
  批次号: string
  模型名称: string
  记录源: string
  上报行数: number
  新增行数: number
  重复行数: number
  更正行数: number
  迟到更正行数: number
  拒绝行数: number
  状态: string
  清单摘要: string
  清单?: BatchDetailRow[]
}

type BatchDetailRow = {
  id: number
  源行标识: string
  业务键: string
  输入类型: string
  Hub装载状态: string
  Link装载状态: string
  Satellite版本: number
  旧值: string
  新值: string
  拒绝原因: string
}

const batches = ref<BatchRow[]>([])
const detail = ref<BatchRow | null>(null)
const openBatch = ref('')
const errorMessage = ref('')

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await fetchJson<{ items: BatchRow[]; total: number }>('/api/vault/batches')
    batches.value = payload.items
    if (openBatch.value) {
      detail.value = await fetchJson<BatchRow>(`/api/vault/batches/${openBatch.value}`)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次清单读取失败'
  }
}

async function toggle(batchNo: string) {
  if (openBatch.value === batchNo) {
    openBatch.value = ''
    detail.value = null
    return
  }
  errorMessage.value = ''
  try {
    openBatch.value = batchNo
    detail.value = await fetchJson<BatchRow>(`/api/vault/batches/${batchNo}`)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次明细读取失败'
  }
}

onMounted(reload)
</script>

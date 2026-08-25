<script setup lang="ts">
import { ref, onMounted, watch, computed, nextTick } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { ElMessageBox, ElMessage } from 'element-plus'
import type { TableInstance } from 'element-plus'
import { ApiUtils, permMap } from '@/api'
import type { DirectoryRecord, FileRecord } from '@/api'
import { useUserStore } from '@/store/user'
import { useLogStore } from '@/store/logs'
import { usePreferenceStore } from '@/store/preferences'
import {
  Document,
  Folder,
  Download,
  Delete,
  Upload,
  CopyDocument,
  Rank,
  InfoFilled,
  Back,
  Setting
} from '@element-plus/icons-vue'
import UploadDialog from '@/components/files/UploadDialog.vue'
import DetailsDialog from '@/components/files/DetailsDialog.vue'
import FileTypeIcon from '@/components/files/FileTypeIcon.vue'
import {
  createConnector,
  formatBytes,
  formatDateTime,
  getLastFilenameStemRange,
  getLastPathComponentRange,
  selectInputRange,
  copyToClipboard
} from '@/utils'

const route = useRoute()
const router = useRouter()
const { t } = useI18n()
const userStore = useUserStore()
const logStore = useLogStore()
const preferenceStore = usePreferenceStore()

const currentPath = computed(() => {
  const path = route.params.path as string | string[]
  if (!path) return ''
  let p = Array.isArray(path) ? path.join('/') : path
  if (!p.endsWith('/')) p += '/'
  return p
})

// const joinPath = (base: string, name: string) => {
//   if (!base) return name
//   return base.endsWith('/') ? `${base}${name}` : `${base}/${name}`
// }

const dirs = ref<DirectoryRecord[]>([])
const files = ref<FileRecord[]>([])
const loading = ref(false)

// Pagination & Sorting
const currentPage = ref(1)
const pageSize = ref(preferenceStore.filePageSize)
const totalItems = ref(0)
const sortBy = ref<string>(preferenceStore.fileSortBy)
const sortDesc = ref<boolean>(preferenceStore.fileSortDesc)
const manualPathInput = ref('')
const isDropActive = ref(false)
const showManualPathControls = ref(false)
const manualPathInputRef = ref<any>(null)
const displayConfigVisible = ref(false)
const fileTableRef = ref<TableInstance>()
const tableRows = computed(() => [
  ...dirs.value.map((d) => ({ ...d, isDir: true })),
  ...files.value.map((f) => ({ ...f, isDir: false }))
])
const fileColumns = computed(() => preferenceStore.fileColumns)

type PathSelectMode = 'last-filename' | 'last-pathname'

const normalizeDirPath = (path: string) => {
  let normalized = path.trim()
  if (!normalized || normalized === '/') return ''
  normalized = normalized.replace(/^\/+/, '')
  if (!normalized.endsWith('/')) normalized += '/'
  return normalized
}

const conn = createConnector(userStore.token)

const scrollTableToTop = async () => {
  await nextTick()
  fileTableRef.value?.setScrollTop(0)
}

const loadData = async () => {
  loading.value = true
  try {
    const offset = (currentPage.value - 1) * pageSize.value
    const [data, count] = await ApiUtils.listPath(conn, currentPath.value, {
      offset,
      limit: pageSize.value,
      orderBy: (sortBy.value || 'none') as any,
      orderDesc: sortDesc.value
    })
    dirs.value = data.dirs
    files.value = data.files
    totalItems.value = count.dirs + count.files
  } catch (e: unknown) {
    const err = e as Error
    logStore.logMessage('error', err.message || 'Failed to load files')
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  manualPathInput.value = currentPath.value
  if (!currentPath.value && preferenceStore.fileLastPath) {
    router.replace(`/files/${preferenceStore.fileLastPath}`)
    return
  }
  loadData()
})
watch(currentPath, () => {
  manualPathInput.value = currentPath.value
  preferenceStore.fileLastPath = currentPath.value
  currentPage.value = 1
  loadData()
})

const handleSortChange = ({ prop, order }: { prop: string, order: string }) => {
  if (!order) {
    sortBy.value = ''
    sortDesc.value = false
  } else {
    sortBy.value = prop;
    sortDesc.value = order === 'descending'
  }
  preferenceStore.fileSortBy = sortBy.value as any
  preferenceStore.fileSortDesc = sortDesc.value
  currentPage.value = 1
  loadData()
}

const goToManualPath = () => {
  const normalized = normalizeDirPath(manualPathInput.value)
  if (!normalized) {
    router.push('/files')
    return
  }
  router.push(`/files/${normalized}`)
}

const toggleManualPathControls = async () => {
  showManualPathControls.value = !showManualPathControls.value
  if (showManualPathControls.value) {
    await nextTick()
    const input = manualPathInputRef.value?.input as HTMLInputElement | undefined
    input?.focus()
  }
}

const copyCurrentPath = () => {
  if (!currentPath.value) return
  copyToClipboard('/' + currentPath.value)
  ElMessage.success(t('files.pathCopied'))
}

const handleSizeChange = (val: number) => {
  pageSize.value = val
  preferenceStore.filePageSize = val
  currentPage.value = 1
  loadData()
  void scrollTableToTop()
}

const handleCurrentChange = (val: number) => {
  currentPage.value = val
  loadData()
  void scrollTableToTop()
}

const handleDirClick = (dir: DirectoryRecord) => {
  router.push(`/files/${dir.url}`)
}

const handleBack = () => {
  if (!currentPath.value) return
  const parts = currentPath.value.split('/').filter(Boolean)
  parts.pop()
  router.push(`/files/${parts.join('/')}`)
}

const focusMessageBoxInputSelection = async (sourcePath: string, selectMode: PathSelectMode) => {
  await nextTick()
  const input = document.querySelector('.el-message-box__input .el-input__inner') as HTMLInputElement | null
  if (!input) return
  const [start, end] = selectMode === 'last-filename'
    ? getLastFilenameStemRange(sourcePath)
    : getLastPathComponentRange(sourcePath)
  selectInputRange(input, start, end)
}

const promptPath = async (
  title: string,
  confirmButtonText: string,
  sourcePath: string,
  selectMode: PathSelectMode,
) => {
  const promptInput = ApiUtils.decodePath(sourcePath)
  const promptTask = ElMessageBox.prompt(t('files.enterDestPath'), title, {
    inputValue: promptInput,
    confirmButtonText,
    cancelButtonText: t('users.cancel'),
  })
  void focusMessageBoxInputSelection(promptInput, selectMode)
  const { value } = (await promptTask) as any
  return String(value || '').trim()
}

const handleDelete = async (item: DirectoryRecord | FileRecord, isDir: boolean) => {
  try {
    const displayName = getItemName(item.url) || ApiUtils.decodePath(item.url)
    await ElMessageBox.confirm(t('files.confirmDeleteNamed', {
      type: isDir ? t('files.directory') : t('files.file'),
      name: displayName
    }), t('files.warningTitle'), {
      type: 'warning'
    })
    const path = item.url
    await conn.delete(isDir && !path.endsWith('/') ? path + '/' : path)
    logStore.logMessage('success', t('files.success'))
    loadData()
  } catch (e: unknown) {
    if (e !== 'cancel') {
      const err = e as Error
      logStore.logMessage('error', err.message || t('files.failed'))
    }
  }
}

// Upload Modal State
const uploadDialogRef = ref<{
  open: () => void
  openWithDrop: (e: DragEvent) => Promise<void>
} | null>(null)

const openUploadDialog = () => {
  uploadDialogRef.value?.open()
}

const handleViewDragOver = (e: DragEvent) => {
  if (!currentPath.value) return
  e.preventDefault()
  isDropActive.value = true
}

const handleViewDragLeave = (e: DragEvent) => {
  const current = e.currentTarget as HTMLElement | null
  const related = e.relatedTarget as Node | null
  if (!current || !related || !current.contains(related)) {
    isDropActive.value = false
  }
}

const handleViewDrop = async (e: DragEvent) => {
  if (!currentPath.value) return
  e.preventDefault()
  isDropActive.value = false
  await uploadDialogRef.value?.openWithDrop(e)
}

const handleDownload = (item: DirectoryRecord | FileRecord, isDir: boolean) => {
  let path = item.url
  if (isDir) {
    if (!path.endsWith('/')) path += '/'
    window.location.href = ApiUtils.getBundleUrl(conn, path)
  } else {
    window.location.href = ApiUtils.getDownloadUrl(conn, path)
  }
}

const handleMove = async (item: DirectoryRecord | FileRecord, isDir: boolean) => {
  try {
    let srcPath = item.url
    if (isDir && !srcPath.endsWith('/')) srcPath += '/'

    const value = await promptPath(
      t('files.moveTitle'),
      t('files.moveAction'),
      srcPath,
      isDir ? 'last-pathname' : 'last-filename'
    )
    if (value) {
      let dstPath = ApiUtils.encodePath(value)
      if (isDir && !dstPath.endsWith('/')) dstPath += '/'
      await conn.move(srcPath, dstPath)
      logStore.logMessage('success', t('files.moveSuccess'))
      loadData()
    }
  } catch (e: unknown) {
    if (e !== 'cancel') {
      const err = e as Error
      logStore.logMessage('error', err.message || t('files.moveFailed'))
    }
  }
}

const handleCopy = async (item: DirectoryRecord | FileRecord, isDir: boolean) => {
  try {
    let srcPath = item.url
    if (isDir && !srcPath.endsWith('/')) srcPath += '/'

    const value = await promptPath(
      t('files.copyTitle'),
      t('files.copyAction'),
      srcPath,
      isDir ? 'last-pathname' : 'last-filename'
    )
    if (value) {
      let dstPath = ApiUtils.encodePath(value)
      if (isDir && !dstPath.endsWith('/')) dstPath += '/'
      await conn.copy(srcPath, dstPath)
      logStore.logMessage('success', t('files.copySuccess'))
      loadData()
    }
  } catch (e: unknown) {
    if (e !== 'cancel') {
      const err = e as Error
      logStore.logMessage('error', err.message || t('files.copyFailed'))
    }
  }
}

const handlePermissionChange = async (file: FileRecord, perm: number) => {
  try {
    await conn.setFilePermission(file.url, perm)
    logStore.logMessage('success', 'Permission updated')
  } catch (e: unknown) {
    const err = e as Error
    logStore.logMessage('error', err.message || 'Failed to update permission')
    loadData() // revert
  }
}

const canManagePermission = (row: DirectoryRecord | FileRecord) => {
  if (row.url.endsWith('/')) return false
  const fileRow = row as FileRecord
  return userStore.userInfo?.is_admin || fileRow.owner_id === userStore.userInfo?.id
}

const isImage = (row: FileRecord) => {
  const ext = row.url.split('.').pop()?.toLowerCase()
  const imgFromExt = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(ext || '')
  return imgFromExt || (row.mime_type ? row.mime_type.startsWith('image/') : false)
}

const isTextFile = (row: FileRecord) => {
  if (row.mime_type && row.mime_type.startsWith('text/')) return true
  const ext = row.url.split('.').pop()?.toLowerCase()
  const txtFromExt = ['txt', 'md', 'json', 'js', 'ts', 'vue', 'html', 'css', 'py', 'sh', 'csv', 'xml'].includes(ext || '')
  return txtFromExt || (row.mime_type ? row.mime_type.startsWith('text/') : false)
}

const handleFileIconClick = (row: FileRecord) => {
  if (isImage(row)) return // Handled by el-image preview
  window.open(ApiUtils.getFullUrl(conn, row.url))
}

const handleFileNameClick = (row: FileRecord) => {
  if (isTextFile(row)) {
    router.push({ name: 'Editor', query: { path: row.url } })
  } else {
    window.location.href = ApiUtils.getFullUrl(conn, row.url)
  }
}

const handleNewCommand = async (command: string) => {
  if (command === 'text') {
    try {
      const { value } = (await ElMessageBox.prompt(t('files.newFilePrompt'), t('files.newTextFile'), {
        confirmButtonText: t('files.createAction'),
        cancelButtonText: t('users.cancel'),
        inputPattern: /^.+$/,
        inputErrorMessage: t('files.newFileNameRequired')
      })) as any
      if (value) {
        const newPath = currentPath.value + value
        router.push({ name: 'Editor', query: { path: newPath } })
      }
    } catch (e) {
      // Cancelled
    }
  }
}

// Details Modal State
const detailsDialogRef = ref<InstanceType<typeof DetailsDialog> | null>(null)

const handleDetails = (row: DirectoryRecord | FileRecord, isDir: boolean) => {
  detailsDialogRef.value?.open(row, isDir)
}

const getItemName = (url: string) => {
  return ApiUtils.decodePath(url).split('/').filter(Boolean).pop() || ''
}
</script>

<template>
  <div class="h-full min-h-0 flex flex-col gap-4">
    <div class="flex justify-between items-start gap-3 flex-wrap">
      <div class="flex items-center gap-2 min-w-0 flex-1">
        <el-button @click="handleBack" :disabled="!currentPath">
          <el-icon><Back /></el-icon>
        </el-button>
        <el-button @click="toggleManualPathControls">
          {{ t('files.pathNav') }}
        </el-button>
        <el-collapse-transition>
          <div v-show="showManualPathControls" class="flex items-center gap-2">
            <el-input
              ref="manualPathInputRef"
              v-model="manualPathInput"
              :placeholder="t('files.pathPlaceholder')"
              class="max-w-md"
              clearable
              @keyup.enter="goToManualPath"
            >
              <template #prepend>/</template>
            </el-input>
            <el-button @click="goToManualPath">{{ t('files.goPath') }}</el-button>
          </div>
        </el-collapse-transition>
        <div class="group flex items-center gap-1 min-w-0">
          <el-breadcrumb separator="/" :separator-class="'text-xs'" class="text-xs leading-5 min-w-0">
            <el-breadcrumb-item :to="{ path: '/files' }">Root</el-breadcrumb-item>
            <el-breadcrumb-item 
              v-for="(part, index) in currentPath.split('/').filter(Boolean)" 
              :key="index"
              :to="{ path: '/files/' + currentPath.split('/').filter(Boolean).slice(0, index + 1).join('/') + '/' }"
            >
              <span class="max-w-[10rem] truncate" :title="ApiUtils.decodePath(part)">{{ ApiUtils.decodePath(part) }}</span>
            </el-breadcrumb-item>
          </el-breadcrumb>
          <el-tooltip :content="t('files.copyPath')" placement="top" :disabled="!currentPath">
            <el-button link :icon="CopyDocument" :disabled="!currentPath" class="text-slate-400 opacity-0 group-hover:opacity-100 transition-opacity" @click="copyCurrentPath" />
          </el-tooltip>
        </div>
      </div>
      <div class="flex gap-2 items-center flex-wrap w-full sm:w-auto sm:justify-end">
        <el-popover
          v-model:visible="displayConfigVisible"
          placement="bottom-end"
          trigger="click"
          :width="260"
        >
          <template #reference>
            <el-button>
              <el-icon class="mr-1"><Setting /></el-icon>
              {{ t('files.display') }}
            </el-button>
          </template>
          <div class="flex flex-col gap-2">
            <span class="text-sm font-medium text-slate-700">{{ t('files.columns') }}</span>
            <el-checkbox v-model="fileColumns.size">{{ t('files.size') }}</el-checkbox>
            <el-checkbox v-model="fileColumns.created">{{ t('files.created') }}</el-checkbox>
            <el-checkbox v-model="fileColumns.accessed">{{ t('files.accessed') }}</el-checkbox>
            <el-checkbox v-model="fileColumns.permission">{{ t('files.permission') }}</el-checkbox>
            <el-checkbox v-model="fileColumns.ownerId">{{ t('files.ownerId') }}</el-checkbox>
          </div>
        </el-popover>
        <el-dropdown @command="handleNewCommand" :disabled="!currentPath">
          <el-button type="success" :disabled="!currentPath">
            <el-icon class="mr-1"><Document /></el-icon>
            {{ t('files.new') }}
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="text">{{ t('files.newTextFile') }}</el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
        <el-button type="primary" @click="openUploadDialog" :disabled="!currentPath">
          <el-icon class="mr-1"><Upload /></el-icon>
          {{ t('files.upload') }}
        </el-button>
      </div>
    </div>

    <el-card
      shadow="never"
      v-loading="loading"
      class="flex-1 min-h-0 transition-colors"
      :class="isDropActive ? 'ring-2 ring-blue-400 bg-blue-50/40' : ''"
      :body-style="{ height: '100%', display: 'flex', flexDirection: 'column' }"
      @dragover="handleViewDragOver"
      @dragleave="handleViewDragLeave"
      @drop="handleViewDrop"
    >
      <div class="flex-1 min-h-0">
      <el-table 
        ref="fileTableRef"
        :data="tableRows" 
        style="width: 100%"
        height="100%"
        :row-style="{ height: '52px' }"
        :cell-style="{ paddingTop: '10px', paddingBottom: '10px' }"
        @sort-change="handleSortChange"
      >
        <el-table-column prop="url" :label="t('files.name')" min-width="200" sortable="custom">
          <template #default="{ row }">
            <div class="flex items-center gap-2 hover:text-blue-500 min-w-0">
              <template v-if="!row.isDir && isImage(row)">
                <el-image 
                  :src="ApiUtils.getThumbUrl(conn, row.url)" 
                  lazy 
                  class="w-8 h-8 rounded cursor-pointer bg-gray-50"
                  fit="cover"
                  :preview-src-list="[ApiUtils.getFullUrl(conn, row.url)]"
                  preview-teleported
                  @click.stop
                >
                  <template #error>
                    <el-icon size="30" color="#909399" class="cursor-pointer rounded p-0.5 bg-gray-50" @click="handleFileIconClick(row)"><Document /></el-icon>
                  </template>
                </el-image>
              </template>
              <el-icon v-else 
                size="30"
                :color="row.isDir ? '#e6a23c' : '#909399'" 
                class="cursor-pointer rounded p-0.5 bg-gray-50" 
                @click="row.isDir ? handleDirClick(row) : handleFileIconClick(row)
                ">
                <Folder v-if="row.isDir" />
                <FileTypeIcon v-else :url="row.url" :mime-type="row.mime_type" />
              </el-icon>
              <span
                class="cursor-pointer truncate block flex-1 min-w-0"
                :title="getItemName(row.url)"
                @click="row.isDir ? handleDirClick(row) : handleFileNameClick(row)"
              >
                {{ getItemName(row.url) }}
              </span>
            </div>
          </template>
        </el-table-column>
        <el-table-column v-if="fileColumns.size" prop="file_size" :label="t('files.size')" width="120" sortable="custom">
          <template #default="{ row }">
            {{ row.isDir ? '-' : formatBytes(row.file_size) }}
          </template>
        </el-table-column>
        <el-table-column v-if="fileColumns.created" prop="create_time" :label="t('files.created')" width="180" sortable="custom">
          <template #default="{ row }">
            {{ formatDateTime(row.create_time) }}
          </template>
        </el-table-column>
        <el-table-column v-if="fileColumns.accessed" prop="access_time" :label="t('files.accessed')" width="180" sortable="custom">
          <template #default="{ row }">
            {{ formatDateTime(row.access_time) }}
          </template>
        </el-table-column>
        <el-table-column v-if="fileColumns.ownerId" :label="t('files.ownerId')" width="120">
          <template #default="{ row }">
            {{ row.isDir ? '-' : row.owner_id }}
          </template>
        </el-table-column>
        <el-table-column v-if="fileColumns.permission" :label="t('files.permission')" width="150">
          <template #default="{ row }">
            <el-select 
              v-if="canManagePermission(row)" 
              v-model="row.permission" 
              size="medium"
              @change="(val: number) => handlePermissionChange(row, val)"
            >
              <el-option v-for="(label, value) in permMap" :key="value" :label="t(`files.permissions.${label}`)" :value="Number(value)" />
            </el-select>
            <span v-else-if="!row.isDir">{{ t(`files.permissions.${permMap[row.permission] || 'unset'}`) }}</span>
            <span v-else>-</span>
          </template>
        </el-table-column>
        <el-table-column :label="t('files.actions')" width="260" fixed="right">
          <template #default="{ row }">
            <el-button-group>
              <el-button size="medium" @click="handleDetails(row, row.isDir)" title="Details">
                <el-icon><InfoFilled /></el-icon>
              </el-button>
              <el-button size="medium" @click="handleDownload(row, row.isDir)" title="Download">
                <el-icon><Download /></el-icon>
              </el-button>
              <el-button size="medium" @click="handleCopy(row, row.isDir)" title="Copy">
                <el-icon><CopyDocument /></el-icon>
              </el-button>
              <el-button size="medium" @click="handleMove(row, row.isDir)" title="Move">
                <el-icon><Rank /></el-icon>
              </el-button>
              <el-button size="medium" type="danger" @click="handleDelete(row, row.isDir)" title="Delete">
                <el-icon><Delete /></el-icon>
              </el-button>
            </el-button-group>
          </template>
        </el-table-column>
        <template #empty>
          <el-empty :description="t('files.empty')" />
        </template>
      </el-table>
      </div>
      
      <div class="mt-4 flex justify-end shrink-0">
        <el-pagination
          v-model:current-page="currentPage"
          v-model:page-size="pageSize"
          :page-sizes="[20, 50, 100, 200, 500, 1000, 2000]"
          layout="total, sizes, prev, pager, next"
          :total="totalItems"
          @size-change="handleSizeChange"
          @current-change="handleCurrentChange"
        />
      </div>
    </el-card>

    <!-- Upload Dialog -->
    <UploadDialog 
      ref="uploadDialogRef" 
      :conn="conn" 
      :current-path="currentPath" 
      @uploaded="loadData" 
    />

    <!-- Details Dialog -->
    <DetailsDialog 
      ref="detailsDialogRef" 
      :conn="conn" 
    />
  </div>
</template>
